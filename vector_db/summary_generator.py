"""
Summary Generator — Creates natural-language text summaries from MongoDB
profile and float documents, optimized for semantic embedding.

These summaries are what get embedded into ChromaDB. They are designed to be:
  - Rich enough for semantic search (keywords like region names, param names)
  - Concise enough to fit within the embedding model's context window
  - Structured for consistent embedding quality
"""

import logging
from vector_db.config import OCEAN_REGIONS

logger = logging.getLogger(__name__)


def detect_ocean_region(lat, lon):
    """
    Determine which ocean region a coordinate falls into.
    Returns the region name or "Indian Ocean" as a fallback.
    """
    if lat is None or lon is None:
        return "Unknown Region"

    for region_name, bounds in OCEAN_REGIONS.items():
        lat_range = bounds["lat"]
        lon_range = bounds["lon"]
        if lat_range[0] <= lat <= lat_range[1] and lon_range[0] <= lon <= lon_range[1]:
            return region_name

    return "Indian Ocean"


def _format_date(timestamp_str):
    """Extract a human-readable date from an ISO timestamp string."""
    if not timestamp_str:
        return "unknown date"
    try:
        # Handle both datetime objects and strings
        if hasattr(timestamp_str, 'strftime'):
            return timestamp_str.strftime('%B %d, %Y')
        # ISO string like "2003-01-10T19:30:03Z"
        from datetime import datetime
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        return dt.strftime('%B %d, %Y')
    except (ValueError, TypeError):
        return str(timestamp_str)[:10] if timestamp_str else "unknown date"


def _format_month_year(timestamp_str):
    """Extract month and year from a timestamp."""
    if not timestamp_str:
        return "unknown"
    try:
        if hasattr(timestamp_str, 'strftime'):
            return timestamp_str.strftime('%B %Y')
        from datetime import datetime
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        return dt.strftime('%B %Y')
    except (ValueError, TypeError):
        return str(timestamp_str)[:7] if timestamp_str else "unknown"


def _data_mode_label(mode):
    """Convert data mode code to human-readable label."""
    modes = {
        'D': 'delayed mode (quality-controlled)',
        'R': 'real-time mode',
        'A': 'adjusted mode',
    }
    return modes.get(mode, f'mode {mode}') if mode else 'unknown mode'


def generate_profile_summary(doc):
    """
    Generate a natural-language summary of an ARGO profile document.

    Args:
        doc: MongoDB profile document (dict)

    Returns:
        str: Text summary for embedding
    """
    platform = doc.get('platform_number', 'Unknown')
    cycle = doc.get('cycle_number', '?')
    direction = 'ascending' if doc.get('direction') == 'A' else 'descending'

    lat = doc.get('latitude')
    lon = doc.get('longitude')
    region = detect_ocean_region(lat, lon)

    lat_str = f"{abs(lat):.2f}°{'N' if lat >= 0 else 'S'}" if lat is not None else "unknown"
    lon_str = f"{abs(lon):.2f}°{'E' if lon >= 0 else 'W'}" if lon is not None else "unknown"

    date_str = _format_date(doc.get('timestamp'))
    month_year = _format_month_year(doc.get('timestamp'))

    max_pres = doc.get('max_pres')
    depth_str = f"0 to {max_pres:.0f} meters" if max_pres else "unknown depth"

    n_levels = doc.get('n_levels', 0)
    data_mode = _data_mode_label(doc.get('data_mode'))

    station_params = doc.get('station_parameters', [])
    params_str = ', '.join(station_params) if station_params else 'PRES, TEMP, PSAL'

    contains_bgc = doc.get('contains_bgc', False)
    bgc_params = doc.get('bgc_parameters', [])

    project = doc.get('project_name', '')
    pi = doc.get('pi_name', '')

    # Build summary
    parts = [
        f"ARGO float {platform}, cycle {cycle}, {direction} profile.",
        f"Located at {lat_str}, {lon_str} in the {region}.",
        f"Measured on {date_str} ({month_year}).",
        f"Depth range: {depth_str} with {n_levels} depth levels.",
        f"Parameters measured: {params_str}.",
        f"Data mode: {data_mode}.",
    ]

    if contains_bgc and bgc_params:
        parts.append(
            f"Contains BGC (biogeochemical) data: {', '.join(bgc_params)}."
        )

    if project:
        parts.append(f"Project: {project}.")
    if pi:
        parts.append(f"Principal Investigator: {pi}.")

    # Profile QC summary
    pres_qc = doc.get('profile_pres_qc')
    temp_qc = doc.get('profile_temp_qc')
    psal_qc = doc.get('profile_psal_qc')
    if pres_qc or temp_qc or psal_qc:
        qc_parts = []
        if pres_qc:
            qc_parts.append(f"pressure QC={pres_qc}")
        if temp_qc:
            qc_parts.append(f"temperature QC={temp_qc}")
        if psal_qc:
            qc_parts.append(f"salinity QC={psal_qc}")
        parts.append(f"Profile quality: {', '.join(qc_parts)}.")

    return ' '.join(parts)


def generate_profile_metadata(doc):
    """
    Extract metadata fields for ChromaDB filtering.

    These fields allow filtering search results without needing
    to do full semantic search. ChromaDB supports exact-match and
    range filters on metadata.

    Args:
        doc: MongoDB profile document (dict)

    Returns:
        dict: Metadata dict suitable for ChromaDB
    """
    lat = doc.get('latitude')
    lon = doc.get('longitude')

    metadata = {
        'profile_id': str(doc.get('_id', '')),
        'platform_number': str(doc.get('platform_number', '')),
        'cycle_number': int(doc.get('cycle_number', 0)) if doc.get('cycle_number') is not None else 0,
        'data_mode': str(doc.get('data_mode', '')),
        'file_type': str(doc.get('file_type', 'core')),
        'contains_bgc': doc.get('contains_bgc', False),
        'region': detect_ocean_region(lat, lon),
        'n_levels': int(doc.get('n_levels', 0)) if doc.get('n_levels') is not None else 0,
    }

    # Numeric fields for range queries
    if lat is not None:
        metadata['latitude'] = float(lat)
    if lon is not None:
        metadata['longitude'] = float(lon)

    max_pres = doc.get('max_pres')
    if max_pres is not None:
        metadata['max_pres'] = float(max_pres)

    # Date components for filtering
    timestamp = doc.get('timestamp')
    if timestamp:
        try:
            if hasattr(timestamp, 'year'):
                metadata['year'] = timestamp.year
                metadata['month'] = timestamp.month
            else:
                from datetime import datetime
                dt = datetime.fromisoformat(str(timestamp).replace('Z', '+00:00'))
                metadata['year'] = dt.year
                metadata['month'] = dt.month
        except (ValueError, TypeError):
            pass

    # Station parameters as a comma-separated string
    # (ChromaDB metadata does not support list values)
    station_params = doc.get('station_parameters', [])
    if station_params:
        metadata['station_parameters'] = ','.join(station_params)

    bgc_params = doc.get('bgc_parameters', [])
    if bgc_params:
        metadata['bgc_parameters'] = ','.join(bgc_params)

    return metadata


def generate_float_summary(doc):
    """
    Generate a natural-language summary of an ARGO float document.

    Args:
        doc: MongoDB float document (dict)

    Returns:
        str: Text summary for embedding
    """
    platform = doc.get('platform_number', 'Unknown')
    platform_type = doc.get('platform_type', 'unknown type')
    project = doc.get('project_name', '')
    pi = doc.get('pi_name', '')
    data_centre = doc.get('data_centre', '')

    total_cycles = doc.get('total_cycles', 0)

    first_date = _format_date(doc.get('first_date'))
    last_date = _format_date(doc.get('last_date'))
    first_my = _format_month_year(doc.get('first_date'))
    last_my = _format_month_year(doc.get('last_date'))

    bb = doc.get('geo_bounding_box', {})
    min_lat = bb.get('min_lat')
    max_lat = bb.get('max_lat')
    min_lon = bb.get('min_lon')
    max_lon = bb.get('max_lon')

    has_bgc = doc.get('has_bgc', False)
    bgc_params = doc.get('bgc_parameters', [])

    # Determine the primary operating region
    if min_lat is not None and max_lat is not None and min_lon is not None and max_lon is not None:
        center_lat = (min_lat + max_lat) / 2
        center_lon = (min_lon + max_lon) / 2
        region = detect_ocean_region(center_lat, center_lon)
        lat_range = f"{abs(min_lat):.1f}°{'N' if min_lat >= 0 else 'S'} to {abs(max_lat):.1f}°{'N' if max_lat >= 0 else 'S'}"
        lon_range = f"{abs(min_lon):.1f}°{'E' if min_lon >= 0 else 'W'} to {abs(max_lon):.1f}°{'E' if max_lon >= 0 else 'W'}"
    else:
        region = "Unknown Region"
        lat_range = "unknown"
        lon_range = "unknown"

    # Build summary
    parts = [
        f"ARGO float {platform} ({platform_type}).",
        f"Operated in the {region}, latitude {lat_range}, longitude {lon_range}.",
        f"Completed {total_cycles} measurement cycles from {first_date} to {last_date} ({first_my} to {last_my}).",
    ]

    if has_bgc:
        bgc_str = ', '.join(bgc_params) if bgc_params else 'various BGC parameters'
        bgc_cycles = doc.get('bgc_cycles', 0)
        parts.append(
            f"Equipped with BGC sensors measuring {bgc_str}. "
            f"{bgc_cycles} BGC profiles available."
        )
    else:
        parts.append("Core float (no BGC sensors).")

    if project:
        parts.append(f"Project: {project}.")
    if pi:
        parts.append(f"Principal Investigator: {pi}.")
    if data_centre:
        parts.append(f"Data centre: {data_centre}.")

    data_modes = doc.get('data_modes_used', [])
    if data_modes:
        parts.append(f"Data modes: {', '.join(data_modes)}.")

    return ' '.join(parts)


def generate_float_metadata(doc):
    """
    Extract metadata fields for a float document for ChromaDB filtering.

    Args:
        doc: MongoDB float document (dict)

    Returns:
        dict: Metadata dict suitable for ChromaDB
    """
    bb = doc.get('geo_bounding_box', {})
    min_lat = bb.get('min_lat')
    max_lat = bb.get('max_lat')
    min_lon = bb.get('min_lon')
    max_lon = bb.get('max_lon')

    metadata = {
        'platform_number': str(doc.get('platform_number', '')),
        'platform_type': str(doc.get('platform_type', '')),
        'has_bgc': doc.get('has_bgc', False),
        'total_cycles': int(doc.get('total_cycles', 0)),
    }

    if min_lat is not None and max_lat is not None:
        center_lat = (min_lat + max_lat) / 2
        center_lon = (min_lon + max_lon) / 2
        metadata['center_lat'] = float(center_lat)
        metadata['center_lon'] = float(center_lon)
        metadata['region'] = detect_ocean_region(center_lat, center_lon)

    if doc.get('project_name'):
        metadata['project_name'] = str(doc['project_name'])
    if doc.get('pi_name'):
        metadata['pi_name'] = str(doc['pi_name'])

    bgc_params = doc.get('bgc_parameters', [])
    if bgc_params:
        metadata['bgc_parameters'] = ','.join(bgc_params)

    data_modes = doc.get('data_modes_used', [])
    if data_modes:
        metadata['data_modes'] = ','.join(data_modes)

    return metadata
