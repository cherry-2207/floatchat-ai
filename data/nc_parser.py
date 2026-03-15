"""
ARGO NetCDF Parser — Reads single NetCDF profile files and returns
structured dicts ready for MongoDB insertion.

Handles:
  - Core files (D/R prefix): PRES, TEMP, PSAL
  - Synthetic BGC files (SD prefix): core + DOXY, CHLA, BBP700, etc.
  - Character array (|S1) decoding
  - JULD → ISO datetime conversion
  - Masked/NaN/fill value → None
  - QC flag byte → integer
"""

import logging
import numpy as np
import netCDF4 as nc
from datetime import datetime, timedelta
from pathlib import Path
from data.config import CORE_PARAMS, BGC_PARAMS, ARGO_REFERENCE_DATE

logger = logging.getLogger(__name__)

# Reference epoch for JULD conversion
_ARGO_EPOCH = datetime(1950, 1, 1, 0, 0, 0)


# ─── Utility Functions ───────────────────────────────────────────────────────

def decode_char_array(var_data):
    """
    Decode a NetCDF character array (|S1 byte array) to a stripped string.
    Handles both single-dim and multi-dim character arrays.
    Returns None if the result is empty/whitespace.
    """
    if var_data is None:
        return None
    try:
        if isinstance(var_data, np.ma.MaskedArray):
            if var_data.mask.all():
                return None
            var_data = var_data.filled(b'')
        if isinstance(var_data, bytes):
            result = var_data.decode('utf-8', errors='replace').strip()
        elif isinstance(var_data, np.ndarray):
            if var_data.dtype.kind == 'S' or var_data.dtype.kind == 'U':
                # Join individual bytes/chars
                result = b''.join(var_data.flat).decode('utf-8', errors='replace').strip()
            else:
                result = str(var_data).strip()
        elif isinstance(var_data, str):
            result = var_data.strip()
        else:
            result = str(var_data).strip()
        
        # Return None for empty strings or strings full of null bytes
        result = result.replace('\x00', '').strip()
        return result if result else None
    except Exception as e:
        logger.debug(f"decode_char_array error: {e}")
        return None


def decode_char_array_2d(var_data, axis=-1):
    """
    Decode a 2D+ character array (e.g., shape (N_PROF, STRING8))
    into a list of strings (one per first-dimension element).
    """
    if var_data is None:
        return []
    try:
        if isinstance(var_data, np.ma.MaskedArray):
            var_data = var_data.filled(b'')
        results = []
        if var_data.ndim == 1:
            results.append(decode_char_array(var_data))
        elif var_data.ndim == 2:
            for i in range(var_data.shape[0]):
                results.append(decode_char_array(var_data[i]))
        elif var_data.ndim == 3:
            for i in range(var_data.shape[0]):
                row = []
                for j in range(var_data.shape[1]):
                    row.append(decode_char_array(var_data[i, j]))
                results.append(row)
        return results
    except Exception as e:
        logger.debug(f"decode_char_array_2d error: {e}")
        return []


def safe_float(value):
    """Convert a numeric value to Python float, returning None for masked/NaN/fill."""
    if value is None:
        return None
    try:
        if isinstance(value, np.ma.core.MaskedConstant):
            return None
        if isinstance(value, (np.ma.MaskedArray,)):
            if value.mask:
                return None
            value = value.item()
        if isinstance(value, np.generic):
            value = value.item()
        if isinstance(value, (int, float)):
            if np.isnan(value) or np.isinf(value):
                return None
            # Common fill values
            if abs(value) > 1e+35 or value == 99999.0 or value == -999.0:
                return None
            return float(value)
        return float(value)
    except (ValueError, TypeError, OverflowError):
        return None


def safe_int(value):
    """Convert to Python int, returning None for invalid values."""
    if value is None:
        return None
    try:
        if isinstance(value, np.ma.core.MaskedConstant):
            return None
        if isinstance(value, (np.ma.MaskedArray,)):
            if value.mask:
                return None
            value = value.item()
        if isinstance(value, np.generic):
            value = value.item()
        return int(value)
    except (ValueError, TypeError, OverflowError):
        return None


def decode_qc_flag(qc_value):
    """
    Decode a QC flag from byte/char to integer.
    ARGO QC: '0'-'9' as chars, ' ' or '' = undefined (-1).
    """
    if qc_value is None:
        return -1
    try:
        if isinstance(qc_value, np.ma.core.MaskedConstant):
            return -1
        if isinstance(qc_value, (np.ma.MaskedArray,)):
            if qc_value.mask:
                return -1
            qc_value = qc_value.item()
        if isinstance(qc_value, bytes):
            qc_value = qc_value.decode('utf-8', errors='replace').strip()
        if isinstance(qc_value, np.bytes_):
            qc_value = qc_value.decode('utf-8', errors='replace').strip()
        if isinstance(qc_value, str):
            qc_value = qc_value.strip()
            if not qc_value:
                return -1
            return int(qc_value)
        if isinstance(qc_value, (int, float, np.integer, np.floating)):
            return int(qc_value)
        return -1
    except (ValueError, TypeError):
        return -1


def juld_to_datetime(juld_value):
    """Convert JULD (days since 1950-01-01) to ISO datetime string."""
    f = safe_float(juld_value)
    if f is None:
        return None
    try:
        dt = _ARGO_EPOCH + timedelta(days=f)
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    except (OverflowError, ValueError):
        return None


def decode_datetime_chars(var_data):
    """Decode a DATE_TIME character array (14 chars: YYYYMMDDHHMMSS) to ISO string."""
    raw = decode_char_array(var_data)
    if not raw or len(raw) < 14:
        return None
    try:
        dt = datetime.strptime(raw[:14], '%Y%m%d%H%M%S')
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    except ValueError:
        return None


# ─── Main Parser ─────────────────────────────────────────────────────────────

class ArgoNCParser:
    """
    Parse a single ARGO NetCDF profile file into a structured dict.
    
    Handles both core (D/R prefix) and synthetic BGC (SD/SR prefix) files.
    Each file may contain multiple profiles (N_PROF dimension), but for
    individual profile files N_PROF is typically 1.
    """

    def __init__(self, filepath):
        self.filepath = Path(filepath)
        self.filename = self.filepath.name
        self.file_type = self._detect_file_type()
    
    def _detect_file_type(self):
        """Detect whether this is a core or synthetic BGC file."""
        name = self.filename
        if name.startswith('SD') or name.startswith('SR'):
            return 'synthetic_bgc'
        elif name.startswith('BD') or name.startswith('BR'):
            return 'bgc'
        else:
            return 'core'
    
    def parse(self):
        """
        Parse the NetCDF file and return a list of profile documents.
        Returns a list because one file can contain multiple profiles (N_PROF > 1).
        """
        try:
            ds = nc.Dataset(str(self.filepath), 'r')
        except Exception as e:
            logger.error(f"Cannot open {self.filepath}: {e}")
            return []
        
        try:
            profiles = []
            n_prof = len(ds.dimensions.get('N_PROF', []))
            if n_prof == 0:
                logger.warning(f"No profiles in {self.filename}")
                return []
            
            for prof_idx in range(n_prof):
                try:
                    doc = self._parse_single_profile(ds, prof_idx)
                    if doc is not None:
                        profiles.append(doc)
                except Exception as e:
                    logger.error(f"Error parsing profile {prof_idx} in {self.filename}: {e}")
            
            return profiles
        finally:
            ds.close()
    
    def _get_var(self, ds, name, default=None):
        """Safely retrieve a variable from the dataset."""
        if name in ds.variables:
            return ds.variables[name]
        return default
    
    def _get_var_data(self, ds, name):
        """Safely retrieve variable data, returning None if missing."""
        var = self._get_var(ds, name)
        if var is None:
            return None
        try:
            return var[:]
        except Exception:
            return None

    def _parse_single_profile(self, ds, prof_idx):
        """Parse a single profile (by index) from the dataset."""
        
        # ── Build profile ID ──
        platform_number = self._extract_string(ds, 'PLATFORM_NUMBER', prof_idx)
        cycle_number = self._extract_int(ds, 'CYCLE_NUMBER', prof_idx)
        direction = self._extract_char(ds, 'DIRECTION', prof_idx)
        
        if not platform_number:
            logger.warning(f"No platform number in {self.filename} profile {prof_idx}")
            return None
        
        # Build unique ID
        profile_id = f"{platform_number}_{cycle_number:03d}" if cycle_number is not None else f"{platform_number}_000"
        if direction == 'D':
            profile_id += 'D'
        if self.file_type == 'synthetic_bgc':
            profile_id += '_BGC'
        
        doc = {"_id": profile_id}
        
        # ── File metadata ──
        doc['data_type'] = decode_char_array(self._get_var_data(ds, 'DATA_TYPE'))
        doc['format_version'] = decode_char_array(self._get_var_data(ds, 'FORMAT_VERSION'))
        doc['handbook_version'] = decode_char_array(self._get_var_data(ds, 'HANDBOOK_VERSION'))
        doc['reference_date_time'] = decode_datetime_chars(
            self._get_var_data(ds, 'REFERENCE_DATE_TIME')
        )
        doc['date_creation'] = decode_datetime_chars(
            self._get_var_data(ds, 'DATE_CREATION')
        )
        doc['date_update'] = decode_datetime_chars(
            self._get_var_data(ds, 'DATE_UPDATE')
        )
        
        # ── Platform info ──
        doc['platform_number'] = platform_number
        doc['project_name'] = self._extract_string(ds, 'PROJECT_NAME', prof_idx)
        doc['pi_name'] = self._extract_string(ds, 'PI_NAME', prof_idx)
        doc['data_centre'] = self._extract_string(ds, 'DATA_CENTRE', prof_idx)
        doc['dc_reference'] = self._extract_string(ds, 'DC_REFERENCE', prof_idx)
        doc['platform_type'] = self._extract_string(ds, 'PLATFORM_TYPE', prof_idx)
        doc['float_serial_no'] = self._extract_string(ds, 'FLOAT_SERIAL_NO', prof_idx)
        doc['firmware_version'] = self._extract_string(ds, 'FIRMWARE_VERSION', prof_idx)
        doc['wmo_inst_type'] = self._extract_string(ds, 'WMO_INST_TYPE', prof_idx)

        # ── Cycle info ──
        doc['cycle_number'] = cycle_number
        doc['direction'] = direction
        doc['data_mode'] = self._extract_char(ds, 'DATA_MODE', prof_idx)
        doc['data_state_indicator'] = self._extract_string(ds, 'DATA_STATE_INDICATOR', prof_idx)
        doc['config_mission_number'] = self._extract_int(ds, 'CONFIG_MISSION_NUMBER', prof_idx)
        doc['vertical_sampling_scheme'] = self._extract_string(
            ds, 'VERTICAL_SAMPLING_SCHEME', prof_idx
        )
        
        # ── Position & Time ──
        juld = self._extract_float(ds, 'JULD', prof_idx)
        doc['timestamp'] = juld_to_datetime(juld)
        doc['timestamp_qc'] = self._extract_qc(ds, 'JULD_QC', prof_idx)
        
        juld_loc = self._extract_float(ds, 'JULD_LOCATION', prof_idx)
        doc['timestamp_location'] = juld_to_datetime(juld_loc)
        
        lat = self._extract_float(ds, 'LATITUDE', prof_idx)
        lon = self._extract_float(ds, 'LONGITUDE', prof_idx)
        doc['latitude'] = lat
        doc['longitude'] = lon
        doc['position_qc'] = self._extract_qc(ds, 'POSITION_QC', prof_idx)
        doc['positioning_system'] = self._extract_string(ds, 'POSITIONING_SYSTEM', prof_idx)
        
        # GeoJSON (MongoDB 2dsphere index requires [lon, lat])
        if lat is not None and lon is not None:
            doc['geo_location'] = {
                'type': 'Point',
                'coordinates': [lon, lat]
            }
        else:
            doc['geo_location'] = None
        
        # ── Profile-level QC ──
        doc['profile_pres_qc'] = self._extract_char(ds, 'PROFILE_PRES_QC', prof_idx)
        doc['profile_temp_qc'] = self._extract_char(ds, 'PROFILE_TEMP_QC', prof_idx)
        doc['profile_psal_qc'] = self._extract_char(ds, 'PROFILE_PSAL_QC', prof_idx)
        
        # BGC profile QC if present
        for bgc_param in BGC_PARAMS:
            qc_name = f'PROFILE_{bgc_param}_QC'
            if qc_name in ds.variables:
                doc[f'profile_{bgc_param.lower()}_qc'] = self._extract_char(
                    ds, qc_name, prof_idx
                )
        
        # ── Station Parameters ──
        station_params = self._extract_station_parameters(ds, prof_idx)
        doc['station_parameters'] = station_params
        
        # ── Detect BGC content ──
        bgc_present = []
        for sp in station_params:
            if sp and sp.upper() in [b.upper() for b in BGC_PARAMS]:
                bgc_present.append(sp.upper())
        doc['contains_bgc'] = len(bgc_present) > 0
        if bgc_present:
            doc['bgc_parameters'] = bgc_present
        
        # ── Parameter Data Mode (BGC files) ──
        if 'PARAMETER_DATA_MODE' in ds.variables:
            pdm = self._get_var_data(ds, 'PARAMETER_DATA_MODE')
            if pdm is not None and prof_idx < pdm.shape[0]:
                modes = []
                for m in pdm[prof_idx]:
                    decoded = decode_char_array(np.atleast_1d(m))
                    modes.append(decoded if decoded else None)
                doc['parameter_data_mode'] = modes

        # ── Measurements ──
        measurements, n_levels = self._extract_measurements(ds, prof_idx, station_params)
        doc['measurements'] = measurements
        doc['n_levels'] = n_levels
        
        # Max pressure
        if measurements:
            pres_values = [m.get('pres') for m in measurements if m.get('pres') is not None]
            doc['max_pres'] = max(pres_values) if pres_values else None
        else:
            doc['max_pres'] = None
        
        # ── Calibration ──
        doc['calibration'] = self._extract_calibration(ds, prof_idx)
        
        # ── History ──
        doc['history'] = self._extract_history(ds, prof_idx)
        
        # ── Ingestion metadata ──
        doc['source_file'] = self.filename
        doc['file_type'] = self.file_type
        doc['ingested_at'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        
        return doc

    # ─── Field extraction helpers ─────────────────────────────────────────

    def _extract_string(self, ds, var_name, prof_idx):
        """Extract a string field for a given profile index."""
        var = self._get_var(ds, var_name)
        if var is None:
            return None
        data = var[:]
        if data.ndim >= 2 and prof_idx < data.shape[0]:
            return decode_char_array(data[prof_idx])
        elif data.ndim == 1:
            return decode_char_array(data)
        return None
    
    def _extract_char(self, ds, var_name, prof_idx):
        """Extract a single character field (like DIRECTION, DATA_MODE)."""
        var = self._get_var(ds, var_name)
        if var is None:
            return None
        data = var[:]
        if data.ndim >= 1 and prof_idx < data.shape[0]:
            val = data[prof_idx]
            if isinstance(val, np.ma.core.MaskedConstant):
                return None
            if isinstance(val, bytes):
                result = val.decode('utf-8', errors='replace').strip()
            elif isinstance(val, np.bytes_):
                result = val.decode('utf-8', errors='replace').strip()
            else:
                result = str(val).strip()
            return result if result else None
        return None
    
    def _extract_float(self, ds, var_name, prof_idx):
        """Extract a float value for a given profile index."""
        var = self._get_var(ds, var_name)
        if var is None:
            return None
        data = var[:]
        if prof_idx < data.shape[0]:
            return safe_float(data[prof_idx])
        return None
    
    def _extract_int(self, ds, var_name, prof_idx):
        """Extract an integer value for a given profile index."""
        var = self._get_var(ds, var_name)
        if var is None:
            return None
        data = var[:]
        if prof_idx < data.shape[0]:
            return safe_int(data[prof_idx])
        return None
    
    def _extract_qc(self, ds, var_name, prof_idx):
        """Extract a QC flag as integer."""
        var = self._get_var(ds, var_name)
        if var is None:
            return -1
        data = var[:]
        if prof_idx < data.shape[0]:
            return decode_qc_flag(data[prof_idx])
        return -1

    def _extract_station_parameters(self, ds, prof_idx):
        """Extract STATION_PARAMETERS as a list of strings."""
        var = self._get_var(ds, 'STATION_PARAMETERS')
        if var is None:
            return CORE_PARAMS.copy()
        
        data = var[:]  # shape: (N_PROF, N_PARAM, STRING16)
        if data.ndim < 3 or prof_idx >= data.shape[0]:
            return CORE_PARAMS.copy()
        
        params = []
        for j in range(data.shape[1]):
            p = decode_char_array(data[prof_idx, j])
            if p:
                params.append(p)
        return params if params else CORE_PARAMS.copy()

    def _extract_measurements(self, ds, prof_idx, station_params):
        """
        Extract measurement arrays for all parameters at each depth level.
        Returns (measurements_list, n_valid_levels).
        
        For each parameter (PRES, TEMP, PSAL, + any BGC):
          - raw value
          - QC flag (as int)
          - adjusted value
          - adjusted QC flag
          - adjusted error
          - dPRES (for BGC synthetic files)
        """
        # Determine N_LEVELS
        pres_var = self._get_var(ds, 'PRES')
        if pres_var is None:
            return [], 0
        
        n_levels_dim = pres_var.shape[-1] if pres_var.ndim >= 2 else pres_var.shape[0]
        
        # Collect all parameter data arrays
        param_data = {}
        all_params = list(set(station_params))  # deduplicate
        
        for param in all_params:
            param_upper = param.upper()
            param_lower = param.lower()
            
            # Raw value
            raw_var = self._get_var(ds, param_upper)
            if raw_var is not None:
                raw_data = raw_var[:]
                if raw_data.ndim >= 2 and prof_idx < raw_data.shape[0]:
                    param_data[param_lower] = raw_data[prof_idx]
                elif raw_data.ndim == 1:
                    param_data[param_lower] = raw_data
            
            # QC
            qc_var = self._get_var(ds, f'{param_upper}_QC')
            if qc_var is not None:
                qc_data = qc_var[:]
                if qc_data.ndim >= 2 and prof_idx < qc_data.shape[0]:
                    param_data[f'{param_lower}_qc'] = qc_data[prof_idx]
                elif qc_data.ndim == 1:
                    param_data[f'{param_lower}_qc'] = qc_data
            
            # Adjusted
            adj_var = self._get_var(ds, f'{param_upper}_ADJUSTED')
            if adj_var is not None:
                adj_data = adj_var[:]
                if adj_data.ndim >= 2 and prof_idx < adj_data.shape[0]:
                    param_data[f'{param_lower}_adjusted'] = adj_data[prof_idx]
                elif adj_data.ndim == 1:
                    param_data[f'{param_lower}_adjusted'] = adj_data
            
            # Adjusted QC
            adj_qc_var = self._get_var(ds, f'{param_upper}_ADJUSTED_QC')
            if adj_qc_var is not None:
                adj_qc_data = adj_qc_var[:]
                if adj_qc_data.ndim >= 2 and prof_idx < adj_qc_data.shape[0]:
                    param_data[f'{param_lower}_adjusted_qc'] = adj_qc_data[prof_idx]
                elif adj_qc_data.ndim == 1:
                    param_data[f'{param_lower}_adjusted_qc'] = adj_qc_data
            
            # Adjusted Error
            err_var = self._get_var(ds, f'{param_upper}_ADJUSTED_ERROR')
            if err_var is not None:
                err_data = err_var[:]
                if err_data.ndim >= 2 and prof_idx < err_data.shape[0]:
                    param_data[f'{param_lower}_adjusted_error'] = err_data[prof_idx]
                elif err_data.ndim == 1:
                    param_data[f'{param_lower}_adjusted_error'] = err_data
            
            # dPRES (synthetic BGC files)
            dpres_var = self._get_var(ds, f'{param_upper}_dPRES')
            if dpres_var is not None:
                dpres_data = dpres_var[:]
                if dpres_data.ndim >= 2 and prof_idx < dpres_data.shape[0]:
                    param_data[f'{param_lower}_dpres'] = dpres_data[prof_idx]
                elif dpres_data.ndim == 1:
                    param_data[f'{param_lower}_dpres'] = dpres_data
        
        # Build per-level measurement dicts
        measurements = []
        for level in range(n_levels_dim):
            meas = {}
            has_any_valid = False
            
            for key, arr in param_data.items():
                if level >= len(arr):
                    continue
                    
                val = arr[level]
                
                if '_qc' in key and not '_adjusted_error' in key:
                    # QC flag
                    meas[key] = decode_qc_flag(val)
                else:
                    # Numeric value
                    converted = safe_float(val)
                    meas[key] = converted
                    # Check if this level has at least one valid core measurement
                    if converted is not None and key in ('pres', 'temp', 'psal'):
                        has_any_valid = True
            
            # For BGC files, also check BGC params for validity
            if not has_any_valid and self.file_type in ('synthetic_bgc', 'bgc'):
                for bgc_p in BGC_PARAMS:
                    if meas.get(bgc_p.lower()) is not None:
                        has_any_valid = True
                        break
            
            # Skip levels where ALL values are masked/None
            if has_any_valid:
                measurements.append(meas)
        
        return measurements, len(measurements)

    def _extract_calibration(self, ds, prof_idx):
        """Extract scientific calibration information."""
        param_var = self._get_var(ds, 'PARAMETER')
        if param_var is None:
            return []
        
        data = param_var[:]
        # shape: (N_PROF, N_CALIB, N_PARAM, STRING16)
        if data.ndim < 4 or prof_idx >= data.shape[0]:
            return []
        
        n_calib = data.shape[1]
        n_param = data.shape[2]
        
        calibrations = []
        for c in range(n_calib):
            for p in range(n_param):
                param_name = decode_char_array(data[prof_idx, c, p])
                if not param_name:
                    continue
                
                calib_entry = {'parameter': param_name}
                
                # Equation
                eq_var = self._get_var(ds, 'SCIENTIFIC_CALIB_EQUATION')
                if eq_var is not None:
                    eq_data = eq_var[:]
                    if (prof_idx < eq_data.shape[0] and c < eq_data.shape[1] 
                            and p < eq_data.shape[2]):
                        calib_entry['equation'] = decode_char_array(
                            eq_data[prof_idx, c, p]
                        )
                
                # Coefficient
                co_var = self._get_var(ds, 'SCIENTIFIC_CALIB_COEFFICIENT')
                if co_var is not None:
                    co_data = co_var[:]
                    if (prof_idx < co_data.shape[0] and c < co_data.shape[1] 
                            and p < co_data.shape[2]):
                        calib_entry['coefficient'] = decode_char_array(
                            co_data[prof_idx, c, p]
                        )
                
                # Comment
                cm_var = self._get_var(ds, 'SCIENTIFIC_CALIB_COMMENT')
                if cm_var is not None:
                    cm_data = cm_var[:]
                    if (prof_idx < cm_data.shape[0] and c < cm_data.shape[1] 
                            and p < cm_data.shape[2]):
                        calib_entry['comment'] = decode_char_array(
                            cm_data[prof_idx, c, p]
                        )
                
                # Date
                dt_var = self._get_var(ds, 'SCIENTIFIC_CALIB_DATE')
                if dt_var is not None:
                    dt_data = dt_var[:]
                    if (prof_idx < dt_data.shape[0] and c < dt_data.shape[1] 
                            and p < dt_data.shape[2]):
                        calib_entry['date'] = decode_datetime_chars(
                            dt_data[prof_idx, c, p]
                        )
                
                calibrations.append(calib_entry)
        
        return calibrations

    def _extract_history(self, ds, prof_idx):
        """Extract history records for this profile."""
        hist_inst = self._get_var(ds, 'HISTORY_INSTITUTION')
        if hist_inst is None:
            return []
        
        data = hist_inst[:]
        # shape: (N_HISTORY, N_PROF, STRING4)
        if data.ndim < 3 or prof_idx >= data.shape[1]:
            return []
        
        n_history = data.shape[0]
        history = []
        
        for h in range(n_history):
            institution = decode_char_array(data[h, prof_idx])
            if not institution:
                continue  # Skip empty history entries
            
            entry = {'institution': institution}
            
            # Extract each history field
            hist_fields = {
                'step': ('HISTORY_STEP', 'string'),
                'software': ('HISTORY_SOFTWARE', 'string'),
                'software_release': ('HISTORY_SOFTWARE_RELEASE', 'string'),
                'reference': ('HISTORY_REFERENCE', 'string'),
                'date': ('HISTORY_DATE', 'datetime'),
                'action': ('HISTORY_ACTION', 'string'),
                'parameter': ('HISTORY_PARAMETER', 'string'),
                'start_pres': ('HISTORY_START_PRES', 'float'),
                'stop_pres': ('HISTORY_STOP_PRES', 'float'),
                'previous_value': ('HISTORY_PREVIOUS_VALUE', 'float'),
                'qctest': ('HISTORY_QCTEST', 'string'),
            }
            
            for field_key, (var_name, field_type) in hist_fields.items():
                var = self._get_var(ds, var_name)
                if var is None:
                    entry[field_key] = None
                    continue
                
                fdata = var[:]
                try:
                    if field_type == 'float':
                        # shape: (N_HISTORY, N_PROF)
                        if h < fdata.shape[0] and prof_idx < fdata.shape[1]:
                            entry[field_key] = safe_float(fdata[h, prof_idx])
                        else:
                            entry[field_key] = None
                    elif field_type == 'datetime':
                        # shape: (N_HISTORY, N_PROF, DATE_TIME)
                        if h < fdata.shape[0] and prof_idx < fdata.shape[1]:
                            entry[field_key] = decode_datetime_chars(fdata[h, prof_idx])
                        else:
                            entry[field_key] = None
                    else:
                        # string: shape (N_HISTORY, N_PROF, STRING_N)
                        if h < fdata.shape[0] and prof_idx < fdata.shape[1]:
                            entry[field_key] = decode_char_array(fdata[h, prof_idx])
                        else:
                            entry[field_key] = None
                except (IndexError, ValueError):
                    entry[field_key] = None
            
            history.append(entry)
        
        return history
