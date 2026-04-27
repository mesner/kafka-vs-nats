#!/usr/bin/env python3
"""
mock_sender.py
────────────────────────────────────────────────────────────────────────────────

Mock HL7 message generator

Feeds:
  DEC  port 2575 — ORU^R01, numeric vitals, ~every 6s (matches 1min
                   send frequency divided by ~10 messages/min observed rate)
  WCM  port 2577 — ORU^R01 with waveform OBRs, ~every 6s (5.5s feed freq)
  (ACM/alarm feed omitted here; add similarly if needed)

Usage:
    python mock_sender.py [--host localhost] [--duration 60]
────────────────────────────────────────────────────────────────────────────────
"""

import socket
import time
import random
import datetime
import threading
import logging
import struct
import argparse
import uuid

# ── CLI ───────────────────────────────────────────────────────────────────────

parser = argparse.ArgumentParser(description='Mock HL7 message sender')
parser.add_argument('--host',     default='localhost', help='Receiver hostname/IP')
parser.add_argument('--duration', type=int, default=0,
                    help='Run for N seconds then stop (0=run forever)')
args,_ = parser.parse_known_args()

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
log = logging.getLogger('mock_sender')

# ── MLLP framing ──────────────────────────────────────────────────────────────

MLLP_START = b'\x0b'
MLLP_END   = b'\x1c\x0d'

def wrap_mllp(hl7: str) -> bytes:
    return MLLP_START + hl7.encode('utf-8') + MLLP_END

def read_ack(conn: socket.socket) -> str | None:
    buf = b''
    conn.settimeout(5.0)
    try:
        while MLLP_END not in buf:
            chunk = conn.recv(1024)
            if not chunk:
                return None
            buf += chunk
        start = buf.index(MLLP_START) + 1
        end   = buf.index(MLLP_END)
        return buf[start:end].decode('utf-8', errors='replace')
    except socket.timeout:
        return None

# ── Feed / device configuration ───────────────────────────────────────────────
#
# The EUI-64 is the device hardware identifier embedded in OBR and OBX.18.

FEEDS = {
    'DEC': 2575,
    'WCM': 2577,
}

DEVICE = {
    'eui':    '3C1A57FFFED657B3',   # EUI-64 of simulated vendor msg
    'model':  'B450',               # shown in OBX.18 equipment field
    'bed':    'ICU-01',             # PV1.3
}

PATIENT = {
    'pid':   '2542',
    'visit': 'V10001',
}

# UTC offset string used in timestamps (-0400 = EDT)
TZ_OFFSET = '-0400'

# ── Timestamp helpers ─────────────────────────────────────────────────────────

def hl7_now() -> str:
    """HL7 datetime with milliseconds and timezone offset."""
    now = datetime.datetime.now()
    ms  = now.strftime('%f')[:4]   # 4 decimal places 
    return now.strftime(f'%Y%m%d%H%M%S.{ms}{TZ_OFFSET}')

def hl7_ts_plain() -> str:
    """HL7 datetime with .0000 milliseconds (used in OBX.14)."""
    return datetime.datetime.now().strftime(f'%Y%m%d%H%M%S.0000{TZ_OFFSET}')

def obr_order_id(feed_type: str) -> str:
    """
    Placer/filler order number in the format observed 
      {EUI}{YYYYMMDDHHmmss.mmmmss}^{FEED}^{EUI}^EUI-64
    """
    now = datetime.datetime.now()
    ts  = now.strftime('%Y%m%d%H%M%S') + f'.{now.strftime("%f")[:4]}'
    eui = DEVICE['eui']
    return f'{eui}{ts}^{feed_type}^{eui}^EUI-64'

# ── OBX.18 equipment identifier ───────────────────────────────────────────────

def equip_id(feed_type: str) -> str:
    """
    OBX.18 format:
      {bed}~{model}^{feed_type}^{EUI}^EUI-64
    e.g. 11C~56^WFM^3C1A57FFFED657B3^EUI-64
    (bed name and device alias abbreviated; model channel shown as short code)
    Using the configured bed/model for clarity.
    """
    return f"{DEVICE['bed']}~{DEVICE['model']}^{feed_type}^{DEVICE['eui']}^EUI-64"

# ── Segment builders ──────────────────────────────────────────────────────────

SEG = '\n'  # vendor uses LF as segment separator 

def build_msh(feed_type: str) -> str:
    """
    MSH per spec 
    MSH.3 = feed type identifier (WFM, DEC)
    MSH.4–6 = empty (vendor default)
    """
    ctrl_id = str(uuid.uuid4())
    return (
        f"MSH|^~\\&|{feed_type}||||{hl7_now()}||ORU^R01^ORU_R01|{ctrl_id}"
        f"|P|2.6|||AL|NE|||||IHE_PCD_001^IHE PCD^1.3.6.1.4.1.19376.1.6.1.1.1^ISO"
    )

def build_pv1() -> str:
    """
    PV1 per spec 
    PV1.2 = I (Inpatient class, spec default)
    PV1.3 = bed location (populated; rest empty)
    No PID segment — vendor omits PID when no ADT patient association.
    """
    return f"PV1||I|{DEVICE['bed']}|||||||||||||||||||||||||||||||||||||||||||||||||"

def build_obr(set_id: int, svc_code: str, svc_name: str,
              svc_sys: str, end_time: str, feed_type: str) -> str:
    """
    OBR per spec section 7.5.
    OBR.7 (obs start) = empty; OBR.8 (obs end) = populated.
    OBR.2 = OBR.3 = placer/filler order number (same value).
    """
    order_id = obr_order_id(feed_type)
    return (
        f"OBR|{set_id}|{order_id}|{order_id}"
        f"|{svc_code}^{svc_name}^{svc_sys}"
        f"|||{end_time}|||||||||||||||||||||||||||||||||||||"
    )

def build_obx_struct(set_id: int, obs_code: str, obs_name: str,
                     obs_sys: str, sub_id: str) -> str:
    """
    Structural OBX (MDS/VMD/CHAN rows) — no value type, no value.
    OBX.11 = X (not applicable).
    """
    return f"OBX|{set_id}||{obs_code}^{obs_name}^{obs_sys}|{sub_id}|||||||X|||||||||"

def build_obx_nm(set_id: int, obs_code: str, obs_name: str, obs_sys: str,
                 sub_id: str, value: str, units_ucum: str,
                 ref_range: str, abn_flag: str,
                 obs_time: str, feed_type: str) -> str:
    """
    Numeric (NM) OBX per spec sections 7.6 and chapter 8.
    Units format: {unit}^{unit}^UCUM  (UCUM triplet).
    OBX.11 = R (result final) when value present, X when empty.
    OBX.14 = observation datetime.
    OBX.18 = equipment instance identifier.
    """
    status = 'R' if value else 'X'
    eq     = equip_id(feed_type)
    return (
        f"OBX|{set_id}|NM|{obs_code}^{obs_name}^{obs_sys}"
        f"|{sub_id}|{value}|{units_ucum}|{ref_range}|{abn_flag}"
        f"|||{status}|||{obs_time}||||{eq}||"
    )

def build_obx_na(set_id: int, obs_code: str, obs_name: str,
                 sub_id: str, wave_values: str, units_ucum: str,
                 obs_time: str, feed_type: str) -> str:
    """
    Waveform (NA = numeric array) OBX.
    Values are ^-delimited signed integers.
    OBX.11 = R.
    """
    eq = equip_id(feed_type)
    return (
        f"OBX|{set_id}|NA|{obs_code}^{obs_name}^MDC"
        f"|{sub_id}|{wave_values}|{units_ucum}|||||R|||{obs_time}||||{eq}||"
    )

def build_obx_nm_attr(set_id: int, obs_code: str, obs_name: str,
                      sub_id: str, value: str, units: str = '') -> str:
    """Waveform attribute OBX (sample rate, data range, sentinels)."""
    return f"OBX|{set_id}|NM|{obs_code}^{obs_name}^MDC|{sub_id}|{value}|{units}|||||R|||||||"

def build_obx_nr_attr(set_id: int, obs_code: str, obs_name: str,
                      sub_id: str, low: str, high: str) -> str:
    """NR (numeric range) OBX for waveform data range."""
    return f"OBX|{set_id}|NR|{obs_code}^{obs_name}^MDC|{sub_id}|{low}^{high}||||||R|||||||"

# ── Abnormality flag helper ───────────────────────────────────────────────────

def abn(value: float, lo: float, hi: float) -> str:
    if value < lo: return 'L'
    if value > hi: return 'H'
    return ''

# ── DEC message builder ───────────────────────────────────────────────────────
#
# Structure from case2_head.hl7 DEC message:
#   MSH → PV1 → OBR|1(monitoring) → OBX MDS→VMD→CHAN→METRIC hierarchy
#
# Sub-ID encoding (IHE PCD-01 hierarchy):
#   MDS  level: 1.0.0.0
#   VMD  level: 1.<VMD_ID>.0.0
#   CHAN level: 1.<VMD_ID>.<CHAN_ID>.0
#   METRIC:     1.<VMD_ID>.<CHAN_ID>.<global_metric_seq>
# The global metric seq is a running counter across all metrics in the message.

def generate_dec() -> str:
    end_ts  = hl7_ts_plain()
    feed    = 'DEC'
    seq     = [0]   # mutable counter shared across helpers

    def next_seq() -> int:
        seq[0] += 1
        return seq[0]

    # Simulated physiological values
    hr   = round(random.gauss(75,  8),  1)
    vpc  = round(max(0, random.gauss(2, 2)), 1)
    spo2 = round(min(100, random.gauss(97, 1)), 1)
    pr   = round(random.gauss(75,  8),  1)
    sqi  = round(min(100, random.gauss(99, 1)), 4)
    # ST segments (uV)
    st = {k: round(random.gauss(0, 20), 0) for k in
          ['I','II','III','V1','V2','V3','V4','V5','V6','AVF','AVL','AVR']}
    # IBP (arterial line chan 1)
    ibp_sys  = round(random.gauss(118, 12), 1)
    ibp_dia  = round(random.gauss(70,  8),  1)
    ibp_mean = round(random.gauss(90,  9),  1)
    ibp_hr   = round(random.gauss(75,  8),  1)
    # Temperature
    temp1 = round(random.gauss(37.0, 0.3), 1)

    segs = [
        build_msh(feed),
        build_pv1(),
        build_obr(1, '182777000', 'monitoring of patient', 'SCT', end_ts, feed),
        # MDS
        build_obx_struct(next_seq(), '69965', 'MDC_DEV_MON_PHYSIO_MULTI_PARAM_MDS', 'MDC', '1.0.0.0'),
        # VMD: ECG (VMD_ID=5 from spec table 8.2)
        build_obx_struct(next_seq(), '69798', 'MDC_DEV_ECG_VMD', 'MDC', '1.5.0.0'),
        build_obx_nm(next_seq(), '147842', 'MDC_ECG_HEART_RATE', 'MDC',
                     '1.5.0.1', str(hr), '{beat}/min^{beat}/min^UCUM', '50-100',
                     abn(hr, 50, 100), end_ts, feed),
        build_obx_nm(next_seq(), '148066', 'MDC_ECG_V_P_C_RATE', 'MDC',
                     '1.5.0.2', str(vpc), '{beat}/min^{beat}/min^UCUM', '',
                     '', end_ts, feed),
        # ST segments
        build_obx_nm(next_seq(), '131841', 'MDC_ECG_AMPL_ST_I',   'MDC', '1.5.0.3',  str(st['I']),   'uV^uV^UCUM', '', '', end_ts, feed),
        build_obx_nm(next_seq(), '131842', 'MDC_ECG_AMPL_ST_II',  'MDC', '1.5.0.4',  str(st['II']),  'uV^uV^UCUM', '', '', end_ts, feed),
        build_obx_nm(next_seq(), '131901', 'MDC_ECG_AMPL_ST_III', 'MDC', '1.5.0.5',  str(st['III']), 'uV^uV^UCUM', '', '', end_ts, feed),
        build_obx_nm(next_seq(), '131843', 'MDC_ECG_AMPL_ST_V1',  'MDC', '1.5.0.6',  str(st['V1']),  'uV^uV^UCUM', '', '', end_ts, feed),
        build_obx_nm(next_seq(), '131844', 'MDC_ECG_AMPL_ST_V2',  'MDC', '1.5.0.7',  str(st['V2']),  'uV^uV^UCUM', '', '', end_ts, feed),
        build_obx_nm(next_seq(), '131845', 'MDC_ECG_AMPL_ST_V3',  'MDC', '1.5.0.8',  str(st['V3']),  'uV^uV^UCUM', '', '', end_ts, feed),
        build_obx_nm(next_seq(), '131846', 'MDC_ECG_AMPL_ST_V4',  'MDC', '1.5.0.9',  str(st['V4']),  'uV^uV^UCUM', '', '', end_ts, feed),
        build_obx_nm(next_seq(), '131847', 'MDC_ECG_AMPL_ST_V5',  'MDC', '1.5.0.10', str(st['V5']),  'uV^uV^UCUM', '', '', end_ts, feed),
        build_obx_nm(next_seq(), '131848', 'MDC_ECG_AMPL_ST_V6',  'MDC', '1.5.0.11', str(st['V6']),  'uV^uV^UCUM', '', '', end_ts, feed),
        build_obx_nm(next_seq(), '131904', 'MDC_ECG_AMPL_ST_AVF', 'MDC', '1.5.0.12', str(st['AVF']), 'uV^uV^UCUM', '', '', end_ts, feed),
        build_obx_nm(next_seq(), '131903', 'MDC_ECG_AMPL_ST_AVL', 'MDC', '1.5.0.13', str(st['AVL']), 'uV^uV^UCUM', '', '', end_ts, feed),
        build_obx_nm(next_seq(), '131902', 'MDC_ECG_AMPL_ST_AVR', 'MDC', '1.5.0.14', str(st['AVR']), 'uV^uV^UCUM', '', '', end_ts, feed),
        # VMD: IBP (VMD_ID=13, CHAN_ID=1 = arterial line chan 1)
        build_obx_struct(next_seq(), '69854', 'MDC_DEV_METER_PRESS_BLD_VMD',  'MDC', '1.13.0.0'),
        build_obx_struct(next_seq(), '69855', 'MDC_DEV_METER_PRESS_BLD_CHAN', 'MDC', '1.13.1.0'),
        build_obx_nm(next_seq(), '150034', 'MDC_PRESS_BLD_ART_DIA',  'MDC',
                     f'1.13.1.{next_seq()-1}', str(ibp_dia),  'mm[Hg]^mm[Hg]^UCUM', '60-90',   abn(ibp_dia,  60,  90),  end_ts, feed),
        build_obx_nm(next_seq(), '150035', 'MDC_PRESS_BLD_ART_MEAN', 'MDC',
                     f'1.13.1.{next_seq()-1}', str(ibp_mean), 'mm[Hg]^mm[Hg]^UCUM', '70-105',  abn(ibp_mean, 70, 105),  end_ts, feed),
        build_obx_nm(next_seq(), '149522', 'MDC_BLD_PULS_RATE_INV',  'MDC',
                     f'1.13.1.{next_seq()-1}', str(ibp_hr),   '{beat}/min^{beat}/min^UCUM', '50-100', abn(ibp_hr, 50, 100), end_ts, feed),
        build_obx_nm(next_seq(), '150033', 'MDC_PRESS_BLD_ART_SYS',  'MDC',
                     f'1.13.1.{next_seq()-1}', str(ibp_sys),  'mm[Hg]^mm[Hg]^UCUM', '90-140',  abn(ibp_sys,  90, 140),  end_ts, feed),
        # VMD: SpO2 (VMD_ID=22)
        build_obx_struct(next_seq(), '69642', 'MDC_DEV_ANALY_SAT_O2_VMD',  'MDC', '1.22.0.0'),
        build_obx_struct(next_seq(), '69643', 'MDC_DEV_ANALY_SAT_O2_CHAN', 'MDC', '1.22.1.0'),
        build_obx_nm(next_seq(), '149530', 'MDC_PULS_OXIM_PULS_RATE', 'MDC',
                     f'1.22.1.{next_seq()-1}', str(pr),   '{beat}/min^{beat}/min^UCUM', '50-100',  abn(pr,   50, 100),  end_ts, feed),
        build_obx_nm(next_seq(), '150456', 'MDC_PULS_OXIM_SAT_O2',    'MDC',
                     f'1.22.1.{next_seq()-1}', str(spo2), '%^%^UCUM',                  '95-100',  abn(spo2, 95, 100),  end_ts, feed),
        build_obx_nm(next_seq(), '160324', 'MDC_SPO2_SIGNAL_QUALITY_INDEX', 'MDC',
                     f'1.22.1.{next_seq()-1}', str(sqi),  '%^%^UCUM',                  '',        '',                  end_ts, feed),
        # VMD: Temperature (VMD_ID=26, CHAN_ID=1)
        build_obx_struct(next_seq(), '69902', 'MDC_DEV_METER_TEMP_VMD',  'MDC', '1.26.0.0'),
        build_obx_struct(next_seq(), '69903', 'MDC_DEV_METER_TEMP_CHAN', 'MDC', '1.26.1.0'),
        build_obx_nm(next_seq(), '150344', 'MDC_TEMP', 'MDC',
                     f'1.26.1.{next_seq()-1}', str(temp1), 'Cel^Cel^UCUM', '36.0-38.0',
                     abn(temp1, 36.0, 38.0), end_ts, feed),
    ]
    return SEG.join(segs) + SEG

# ── WCM message builder ───────────────────────────────────────────────────────
#
# WCM = DEC numeric section (OBR|1) + one OBR per waveform lead (OBR|2+).
# Each waveform OBR contains:
#   OBX MDS struct → VMD struct → NA waveform values
#   + sub-OBX: sample rate, data range, invalid sentinel, missing sentinel, scale factor
#
# From spec section 8.1 and case1 WCM messages:
#   MDC_ATTR_SAMPLE_RATE  68320  — 240 Hz for vendor ECG
#   MDC_ATTR_DATA_RANGE   68323  — NR type: -32751^32767
#   MDC_EVT_DATA_INVALID  197376 — sentinel value (multiple per lead)
#   MDC_EVT_DATA_MISSING  197378 — sentinel value (multiple per lead)
#   MDC_ATTR_SA_MSMT_RES  67945  — scale factor 2.44 uV/LSB
#   waveform units: uV^uV^UCUM
#
# Waveform OBR.4:
#   69121^MDC_OBS_WAVE_CTS^MDC
#
# OBR.8 = end timestamp of waveform batch
# OBR.7 = start timestamp (end - ~6s)

SAMPLE_RATE = 240   # Hz
BATCH_SECS  = 6     # WCM batch window = ~6s at 5.5s feed freq
SAMPLES_PER_BATCH = SAMPLE_RATE * BATCH_SECS  # 1440 samples

# ECG lead definitions matching spec table 8.1
ECG_LEADS = [
    ('131329', 'MDC_ECG_ELEC_POTL_I'),
    ('131330', 'MDC_ECG_ELEC_POTL_II'),
    ('131389', 'MDC_ECG_ELEC_POTL_III'),
    ('131390', 'MDC_ECG_ELEC_POTL_AVR'),
    ('131391', 'MDC_ECG_ELEC_POTL_AVL'),
    ('131392', 'MDC_ECG_ELEC_POTL_AVF'),
    ('131331', 'MDC_ECG_ELEC_POTL_V1'),
    ('131332', 'MDC_ECG_ELEC_POTL_V2'),
    ('131333', 'MDC_ECG_ELEC_POTL_V3'),
    ('131334', 'MDC_ECG_ELEC_POTL_V4'),
    ('131335', 'MDC_ECG_ELEC_POTL_V5'),
    ('131336', 'MDC_ECG_ELEC_POTL_V6'),
]

# Sentinel values observed in case1 (invalid/missing marker values)
DATA_INVALID_SENTINELS = [-32768, -32766, -32765, -32764, -32760,
                           -32759, -32758, -32757, -32756, -32755, -32754]
DATA_MISSING_SENTINELS = [-32767, -32763, -32762, -32761, -32753, -32752]


def simulate_ecg_lead(n_samples: int, amplitude: int = 300) -> list[int]:
    """Simulate one ECG lead as 16-bit signed integers (scale: 2.44 uV/LSB)."""
    samples = []
    for i in range(n_samples):
        # Simple sine-based QRS approximation
        t    = i / SAMPLE_RATE
        hr   = 1.2   # ~72 bpm
        val  = int(amplitude * (
            0.1 * random.gauss(0, 0.3) +
            0.9 * (abs(((t * hr) % 1.0) - 0.5) < 0.05) * random.gauss(0, 1)
        ))
        val  = max(-32000, min(32000, val))
        samples.append(val)
    return samples


def generate_wcm() -> str:
    feed   = 'WCM'
    end_ts = hl7_ts_plain()
    # Start time is end - BATCH_SECS
    end_dt = datetime.datetime.now()
    start_dt = end_dt - datetime.timedelta(seconds=BATCH_SECS)
    start_ts = start_dt.strftime(f'%Y%m%d%H%M%S.0000{TZ_OFFSET}')

    segs = [
        build_msh(feed),
        build_pv1(),
    ]

    # ── OBR|1 — numeric vitals (same as DEC) ─────────────────────────────────
    seq = [0]
    def ns() -> int:
        seq[0] += 1
        return seq[0]

    hr   = round(random.gauss(75,  8),  1)
    spo2 = round(min(100, random.gauss(97, 1)), 1)
    ibp_sys  = round(random.gauss(118, 12), 1)
    ibp_dia  = round(random.gauss(70,  8),  1)
    ibp_mean = round(random.gauss(90,  9),  1)
    ibp_hr   = round(random.gauss(75,  8),  1)

    segs.append(build_obr(1, '182777000', 'monitoring of patient', 'SCT', end_ts, feed))
    segs.append(build_obx_struct(ns(), '69965', 'MDC_DEV_MON_PHYSIO_MULTI_PARAM_MDS', 'MDC', '1.0.0.0'))
    segs.append(build_obx_struct(ns(), '69798', 'MDC_DEV_ECG_VMD', 'MDC', '1.5.0.0'))
    segs.append(build_obx_nm(ns(), '147842', 'MDC_ECG_HEART_RATE', 'MDC',
                              '1.5.0.1', str(hr), '{beat}/min^{beat}/min^UCUM',
                              '50-100', abn(hr, 50, 100), end_ts, feed))
    segs.append(build_obx_struct(ns(), '69854', 'MDC_DEV_METER_PRESS_BLD_VMD',  'MDC', '1.13.0.0'))
    segs.append(build_obx_struct(ns(), '69855', 'MDC_DEV_METER_PRESS_BLD_CHAN', 'MDC', '1.13.1.0'))
    segs.append(build_obx_nm(ns(), '150034', 'MDC_PRESS_BLD_ART_DIA',  'MDC',
                              '1.13.1.3', str(ibp_dia),  'mm[Hg]^mm[Hg]^UCUM', '', '', end_ts, feed))
    segs.append(build_obx_nm(ns(), '150035', 'MDC_PRESS_BLD_ART_MEAN', 'MDC',
                              '1.13.1.4', str(ibp_mean), 'mm[Hg]^mm[Hg]^UCUM', '', '', end_ts, feed))
    segs.append(build_obx_nm(ns(), '149522', 'MDC_BLD_PULS_RATE_INV',  'MDC',
                              '1.13.1.5', str(ibp_hr),   '{beat}/min^{beat}/min^UCUM', '', '', end_ts, feed))
    segs.append(build_obx_nm(ns(), '150033', 'MDC_PRESS_BLD_ART_SYS',  'MDC',
                              '1.13.1.6', str(ibp_sys),  'mm[Hg]^mm[Hg]^UCUM', '', '', end_ts, feed))
    segs.append(build_obx_struct(ns(), '69642', 'MDC_DEV_ANALY_SAT_O2_VMD',  'MDC', '1.22.0.0'))
    segs.append(build_obx_struct(ns(), '69643', 'MDC_DEV_ANALY_SAT_O2_CHAN', 'MDC', '1.22.1.0'))
    segs.append(build_obx_nm(ns(), '149530', 'MDC_PULS_OXIM_PULS_RATE', 'MDC',
                              '1.22.1.11', str(round(random.gauss(75,8),1)),
                              '{beat}/min^{beat}/min^UCUM', '', '', end_ts, feed))
    segs.append(build_obx_nm(ns(), '150456', 'MDC_PULS_OXIM_SAT_O2',    'MDC',
                              '1.22.1.12', str(spo2), '%^%^UCUM', '', '', end_ts, feed))
    segs.append(build_obx_nm(ns(), '160324', 'MDC_SPO2_SIGNAL_QUALITY_INDEX', 'MDC',
                              '1.22.1.13', '99.89999', '%^%^UCUM', '', '', end_ts, feed))

    # ── OBR|2+ — one per ECG lead waveform ───────────────────────────────────
    wf_obr_seq = 2
    for lead_code, lead_name in ECG_LEADS:
        samples = simulate_ecg_lead(SAMPLES_PER_BATCH)
        wave_str = '^'.join(str(s) for s in samples)

        # Sub-IDs for waveform OBX tree within this OBR
        # Convention from case1: 1.5.0.1 for waveform, then .1 .2 .3 for attrs
        wave_sub   = '1.5.0.1'
        rate_sub   = '1.5.0.1.1'
        range_sub  = '1.5.0.1.2'
        scale_sub  = '1.5.0.1.3'

        segs.append(build_obr(wf_obr_seq, '69121', 'MDC_OBS_WAVE_CTS', 'MDC',
                               end_ts, feed))
        segs.append(build_obx_struct(1, '69965', 'MDC_DEV_MON_PHYSIO_MULTI_PARAM_MDS', 'MDC', '1.0.0.0'))
        segs.append(build_obx_struct(2, '69798', 'MDC_DEV_ECG_VMD', 'MDC', '1.5.0.0'))
        segs.append(build_obx_na(3, lead_code, lead_name,
                                  wave_sub, wave_str, 'uV^uV^UCUM', start_ts, feed))
        segs.append(build_obx_nm_attr(4, '68320', 'MDC_ATTR_SAMPLE_RATE',
                                       rate_sub, str(SAMPLE_RATE), 's^s^UCUM'))
        segs.append(build_obx_nr_attr(5, '68323', 'MDC_ATTR_DATA_RANGE',
                                       range_sub, '-32751', '32767'))
        # Invalid sentinels
        attr_seq = 1
        for sv in DATA_INVALID_SENTINELS:
            segs.append(build_obx_nm_attr(5 + attr_seq,
                                           '197376', 'MDC_EVT_DATA_INVALID',
                                           f'{range_sub}.{attr_seq}', str(sv)))
            attr_seq += 1
        # Missing sentinels
        for sv in DATA_MISSING_SENTINELS:
            segs.append(build_obx_nm_attr(5 + attr_seq,
                                           '197378', 'MDC_EVT_DATA_MISSING',
                                           f'{range_sub}.{attr_seq}', str(sv)))
            attr_seq += 1
        # Scale factor (2.44 uV/LSB per spec note section 8.1)
        segs.append(build_obx_nm_attr(5 + attr_seq,
                                       '67945', 'MDC_ATTR_SA_MSMT_RES',
                                       scale_sub, '2.44'))
        wf_obr_seq += 1

    return SEG.join(segs) + SEG

# ── MLLP sender thread ────────────────────────────────────────────────────────

def run_feed(feed_name: str, port: int, generator_fn,
             interval: float, stop_event: threading.Event):
    """Maintain a persistent MLLP connection and push messages at interval."""
    feed_log = logging.getLogger(f'mock.{feed_name}')
    sent = 0
    acked = 0
    errors = 0

    while not stop_event.is_set():
        try:
            feed_log.info(f"Connecting to {args.host}:{port}...")
            with socket.create_connection((args.host, port), timeout=10) as conn:
                feed_log.info(f"Connected to {args.host}:{port}")
                while not stop_event.is_set():
                    hl7_msg = generator_fn()
                    try:
                        conn.sendall(wrap_mllp(hl7_msg))
                        sent += 1

                        ack = read_ack(conn)
                        if ack is None:
                            feed_log.warning("No ACK received — reconnecting")
                            errors += 1
                            break

                        ack_code = ''
                        # Receiver builds ACK with \r segment delimiters
                        for line in ack.replace('\r', '\n').split('\n'):
                            if line.startswith('MSA'):
                                parts = line.split('|')
                                ack_code = parts[1] if len(parts) > 1 else ''
                        if ack_code == 'AA':
                            acked += 1
                        else:
                            feed_log.warning(f"Non-AA ACK: {ack_code!r}")
                            errors += 1

                        feed_log.info(f"Sent {feed_name} #{sent} → ACK={ack_code}")
                        stop_event.wait(timeout=interval)

                    except OSError as e:
                        feed_log.error(f"Send error: {e}")
                        errors += 1
                        break

        except OSError as e:
            feed_log.warning(f"Cannot connect to {args.host}:{port}: {e} — retry in 5s")
            stop_event.wait(timeout=5)

    feed_log.info(f"Feed stopped. sent={sent} acked={acked} errors={errors}")

# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    log.info("=" * 62)
    log.info("Mock vendor Aggregator sender")
    log.info(f"Target : {args.host}")
    log.info(f"Device : EUI-64={DEVICE['eui']}  model={DEVICE['model']}  bed={DEVICE['bed']}")
    log.info(f"Feeds  : DEC (port {FEEDS['DEC']})  WCM (port {FEEDS['WCM']})")
    log.info(f"Duration: {'forever' if args.duration == 0 else f'{args.duration}s'}")
    log.info("=" * 62)

    stop_event = threading.Event()
    feed_specs = [
        ('DEC', FEEDS['DEC'], generate_dec, 6.0),
        ('WCM', FEEDS['WCM'], generate_wcm, 6.0),
    ]

    threads = []
    for name, port, gen_fn, interval in feed_specs:
        t = threading.Thread(
            target=run_feed,
            args=(name, port, gen_fn, interval, stop_event),
            daemon=True,
            name=f'feed-{name}',
        )
        t.start()
        threads.append(t)

    try:
        if args.duration > 0:
            time.sleep(args.duration)
            stop_event.set()
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        stop_event.set()
        log.info("Stopped.")
