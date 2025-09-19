import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import firebase_admin
from firebase_admin import credentials, firestore

# Ansh brand colors
ANSH_COLORS = {
    'primary': '#A66081',
    'secondary': '#C58392', 
    'light': '#F1E0E4',
    'dark': '#8B4D6B',
    'accent': '#E8B4CC'
}

# Page config
st.set_page_config(
    page_title="Ansh KMC Dashboard",
    page_icon="👶",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown(f"""
<style>
    .main-header {{
        background: linear-gradient(90deg, {ANSH_COLORS['primary']} 0%, {ANSH_COLORS['secondary']} 100%);
        padding: 2rem;
        border-radius: 15px;
        color: white;
        margin-bottom: 2rem;
        text-align: center;
    }}
    .metric-container {{
        background: {ANSH_COLORS['light']};
        padding: 1.5rem;
        border-radius: 10px;
        border-left: 5px solid {ANSH_COLORS['primary']};
        margin: 1rem 0;
    }}
    .stTabs [data-baseweb="tab"] {{
        background: {ANSH_COLORS['light']};
        color: {ANSH_COLORS['dark']};
        border-radius: 10px 10px 0 0;
        margin-right: 5px;
    }}
    .stTabs [data-baseweb="tab"]:hover {{
        background: {ANSH_COLORS['secondary']};
        color: white;
    }}
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def initialize_firebase():
    """Initialize Firebase connection"""
    if not firebase_admin._apps:
        try:
            import os

            # Try to use Streamlit secrets first (for deployed version)
            try:
                # Handle private key formatting for different sources
                private_key = st.secrets["firebase"]["private_key"]

                # Clean up private key formatting - handle both escaped and literal newlines
                if "\\n" in private_key:
                    private_key = private_key.replace('\\n', '\n')

                # Ensure proper PEM formatting
                if not private_key.startswith('-----BEGIN PRIVATE KEY-----'):
                    private_key = '-----BEGIN PRIVATE KEY-----\n' + private_key
                if not private_key.endswith('-----END PRIVATE KEY-----'):
                    private_key = private_key + '\n-----END PRIVATE KEY-----'

                firebase_key = {
                    "type": st.secrets["firebase"]["type"],
                    "project_id": st.secrets["firebase"]["project_id"],
                    "private_key_id": st.secrets["firebase"]["private_key_id"],
                    "private_key": private_key,
                    "client_email": st.secrets["firebase"]["client_email"],
                    "client_id": st.secrets["firebase"]["client_id"],
                    "auth_uri": st.secrets["firebase"]["auth_uri"],
                    "token_uri": st.secrets["firebase"]["token_uri"],
                    "auth_provider_x509_cert_url": st.secrets["firebase"]["auth_provider_x509_cert_url"],
                    "client_x509_cert_url": st.secrets["firebase"]["client_x509_cert_url"],
                    "universe_domain": st.secrets["firebase"]["universe_domain"]
                }
                cred = credentials.Certificate(firebase_key)
                st.success("Using Streamlit secrets for Firebase connection")
            except (KeyError, FileNotFoundError, Exception) as e:
                # Fallback to local file (for local development)
                st.info(f"Streamlit secrets not available ({str(e)[:100]}...), trying local file...")
                key_path = '/Users/supriyabansal/Desktop/ansh-kmc-streamlit/firebase-key.json'
                if not os.path.exists(key_path):
                    st.error(f"Firebase key file not found at: {key_path}")
                    st.error("Please ensure firebase-key.json exists locally or configure Streamlit secrets for deployment")
                    return None
                cred = credentials.Certificate(key_path)
                st.success("✅ Using local Firebase key file for development")

            firebase_admin.initialize_app(cred)
            db = firestore.client()
            st.success("Firebase connection established successfully!")
            return db

        except Exception as e:
            st.error(f"Firebase connection failed: {e}")
            st.error("Please check your Firebase configuration and network connection.")
            return None

    try:
        db = firestore.client()
        return db
    except Exception as e:
        st.error(f"Failed to get Firestore client: {e}")
        return None

def convert_unix_to_datetime(timestamp):
    """Convert UNIX timestamp to datetime"""
    if not timestamp:
        return None
    
    if isinstance(timestamp, (int, float)):
        if timestamp > 1000000000000:
            return datetime.fromtimestamp(timestamp / 1000)
        else:
            return datetime.fromtimestamp(timestamp)
    
    return pd.to_datetime(timestamp, errors='coerce')

def load_collection_with_retry(db, collection_name, max_retries=5, batch_size=100):
    """Load a collection with retry logic to handle the _retry error and timeouts"""
    import time

    for attempt in range(max_retries):
        try:
            # Add delay between retries for better network handling
            if attempt > 0:
                delay = min(2 ** attempt, 10)  # Exponential backoff, max 10 seconds
                time.sleep(delay)

            if attempt == 0:
                # First attempt: try normal get()
                docs = db.collection(collection_name).get()
                return docs
            elif attempt == 1:
                # Second attempt: try with timeout and smaller limit
                docs = db.collection(collection_name).limit(batch_size).get()
                return docs
            elif attempt == 2:
                # Third attempt: try with even smaller limit
                docs = db.collection(collection_name).limit(50).get()
                return docs
            else:
                # Final attempts: try with very small limits
                docs = db.collection(collection_name).limit(20).get()
                return docs

        except Exception as e:
            error_msg = str(e).lower()
            if attempt < max_retries - 1:
                if 'timeout' in error_msg or 'retry' in error_msg or 'deadline' in error_msg:
                    st.warning(f"Network timeout for {collection_name} (attempt {attempt + 1}/{max_retries}). Retrying with smaller batch...")
                else:
                    st.warning(f"Error loading {collection_name} (attempt {attempt + 1}/{max_retries}): {str(e)[:100]}... Retrying...")
                continue
            else:
                st.error(f"All attempts failed for {collection_name}: {str(e)[:200]}")
                return []

@st.cache_data(ttl=300)
def load_firebase_data():
    """Load data from Firebase collections"""
    db = initialize_firebase()

    if not db:
        return [], [], []

    try:
        baby_data = []
        discharge_data = []
        followup_data = []

        # Load baby collection
        progress_bar = st.progress(0)
        st.text("Loading baby collection...")

        try:
            baby_docs = load_collection_with_retry(db, 'baby')
            for doc in baby_docs:
                data = doc.to_dict()
                data['id'] = doc.id
                data['source'] = 'baby'
                baby_data.append(data)
        except Exception as e:
            st.warning(f"Could not load baby collection: {e}")

        progress_bar.progress(25)

        # Load babyBackUp collection
        st.text("Loading babyBackUp collection...")
        try:
            backup_docs = load_collection_with_retry(db, 'babyBackUp')
            for doc in backup_docs:
                data = doc.to_dict()
                data['id'] = doc.id
                data['source'] = 'babyBackUp'
                baby_data.append(data)
        except Exception as e:
            st.warning(f"Could not load backup collection: {e}")

        progress_bar.progress(50)

        # Load discharge collection
        st.text("Loading discharge collection...")
        try:
            discharge_docs = load_collection_with_retry(db, 'discharges')
            for doc in discharge_docs:
                data = doc.to_dict()
                data['id'] = doc.id
                discharge_data.append(data)
        except Exception as e:
            st.warning(f"Could not load discharge collection: {e}")

        progress_bar.progress(75)

        # Load follow_up collection
        st.text("Loading follow_up collection...")
        try:
            followup_docs = load_collection_with_retry(db, 'follow_up')
            for doc in followup_docs:
                data = doc.to_dict()
                data['id'] = doc.id
                followup_data.append(data)
        except Exception as e:
            st.warning(f"Could not load follow_up collection: {e}")
        
        progress_bar.progress(100)
        st.text("")
        
        # Filter out test hospitals
        filtered_baby_data = []
        for baby in baby_data:
            hospital_name = baby.get('hospitalName', '').lower()
            if hospital_name and not any(term in hospital_name for term in ['test', 'training', 'demo']):
                filtered_baby_data.append(baby)
        
        st.success(f"Loaded {len(filtered_baby_data)} babies, {len(discharge_data)} discharge records, and {len(followup_data)} follow-up records from {len(set(baby.get('hospitalName') for baby in filtered_baby_data if baby.get('hospitalName')))} hospitals")
        return filtered_baby_data, discharge_data, followup_data
        
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return [], [], []

def calculate_registration_timeliness(baby_data):
    """Calculate registration timeliness KPIs"""
    inborn_babies = [baby for baby in baby_data 
                    if baby.get('placeOfDelivery') in ['यह अस्पताल', 'this hospital']]
    
    within_24h = 0
    within_12h = 0
    
    for baby in inborn_babies:
        birth_time = convert_unix_to_datetime(baby.get('dateOfBirth'))
        reg_time = convert_unix_to_datetime(baby.get('registrationDate') or 
                                         baby.get('registrationDataType', {}).get('registrationDate'))
        
        if birth_time and reg_time:
            time_diff = (reg_time - birth_time).total_seconds() / 3600  # hours
            
            if 0 <= time_diff <= 24:
                within_24h += 1
                if time_diff <= 12:
                    within_12h += 1
    
    total_inborn = len(inborn_babies)
    
    return {
        'total_inborn': total_inborn,
        'within_24h_count': within_24h,
        'within_24h_percentage': (within_24h / total_inborn * 100) if total_inborn > 0 else 0,
        'within_12h_count': within_12h,
        'within_12h_percentage': (within_12h / total_inborn * 100) if total_inborn > 0 else 0
    }

def calculate_kmc_initiation_metrics(baby_data):
    """Calculate KMC initiation timing metrics categorized by inborn/outborn and location"""
    initiation_data = []

    for baby in baby_data:
        uid = baby.get('UID')
        if not uid:
            continue

        birth_date = convert_unix_to_datetime(baby.get('dateOfBirth'))
        if not birth_date:
            continue

        # Determine inborn/outborn status
        place_of_delivery = baby.get('placeOfDelivery', '')
        is_inborn = place_of_delivery in ['यह अस्पताल', 'this hospital']

        # Get current location
        current_location = baby.get('currentLocationOfTheBaby', 'Unknown')

        # Find first KMC session
        first_kmc_date = None
        first_kmc_hours = 0

        for obs_day in baby.get('observationDay', []):
            if obs_day.get('totalKMCtimeDay', 0) > 0:
                age_day = obs_day.get('ageDay', 0)
                kmc_date = birth_date + timedelta(days=age_day)

                if first_kmc_date is None or kmc_date < first_kmc_date:
                    first_kmc_date = kmc_date
                    first_kmc_hours = obs_day.get('totalKMCtimeDay', 0) / 60

        if first_kmc_date:
            time_to_initiation = (first_kmc_date - birth_date).total_seconds() / 3600  # hours
            initiation_data.append({
                'UID': uid,
                'hospital': baby.get('hospitalName', 'Unknown'),
                'birth_date': birth_date,
                'first_kmc_date': first_kmc_date,
                'time_to_initiation_hours': time_to_initiation,
                'first_kmc_hours': first_kmc_hours,
                'is_inborn': is_inborn,
                'delivery_type': 'Inborn' if is_inborn else 'Outborn',
                'current_location': current_location
            })
    
    if not initiation_data:
        return {
            'total_babies_with_kmc': 0,
            'avg_time_to_initiation_hours': 0,
            'within_24h_count': 0,
            'within_24h_percentage': 0,
            'within_48h_count': 0,
            'within_48h_percentage': 0,
            'initiation_data': [],
            'inborn_stats': {},
            'outborn_stats': {},
            'inborn_location_stats': {}
        }

    total_babies = len(initiation_data)
    avg_time = sum(d['time_to_initiation_hours'] for d in initiation_data) / total_babies

    within_24h = len([d for d in initiation_data if d['time_to_initiation_hours'] <= 24])
    within_48h = len([d for d in initiation_data if d['time_to_initiation_hours'] <= 48])

    # Categorize by inborn/outborn
    inborn_data = [d for d in initiation_data if d['is_inborn']]
    outborn_data = [d for d in initiation_data if not d['is_inborn']]

    # Inborn statistics
    inborn_stats = {}
    if inborn_data:
        inborn_stats = {
            'count': len(inborn_data),
            'avg_time_hours': sum(d['time_to_initiation_hours'] for d in inborn_data) / len(inborn_data),
            'within_24h_count': len([d for d in inborn_data if d['time_to_initiation_hours'] <= 24]),
            'within_48h_count': len([d for d in inborn_data if d['time_to_initiation_hours'] <= 48])
        }
        inborn_stats['within_24h_percentage'] = (inborn_stats['within_24h_count'] / inborn_stats['count'] * 100)
        inborn_stats['within_48h_percentage'] = (inborn_stats['within_48h_count'] / inborn_stats['count'] * 100)

    # Outborn statistics
    outborn_stats = {}
    if outborn_data:
        outborn_stats = {
            'count': len(outborn_data),
            'avg_time_hours': sum(d['time_to_initiation_hours'] for d in outborn_data) / len(outborn_data),
            'within_24h_count': len([d for d in outborn_data if d['time_to_initiation_hours'] <= 24]),
            'within_48h_count': len([d for d in outborn_data if d['time_to_initiation_hours'] <= 48])
        }
        outborn_stats['within_24h_percentage'] = (outborn_stats['within_24h_count'] / outborn_stats['count'] * 100)
        outborn_stats['within_48h_percentage'] = (outborn_stats['within_48h_count'] / outborn_stats['count'] * 100)

    # Inborn by location statistics
    inborn_location_stats = {}
    if inborn_data:
        locations = set(d['current_location'] for d in inborn_data)
        for location in locations:
            location_data = [d for d in inborn_data if d['current_location'] == location]
            if location_data:
                inborn_location_stats[location] = {
                    'count': len(location_data),
                    'avg_time_hours': sum(d['time_to_initiation_hours'] for d in location_data) / len(location_data),
                    'within_24h_count': len([d for d in location_data if d['time_to_initiation_hours'] <= 24]),
                    'within_48h_count': len([d for d in location_data if d['time_to_initiation_hours'] <= 48])
                }
                inborn_location_stats[location]['within_24h_percentage'] = (
                    inborn_location_stats[location]['within_24h_count'] / inborn_location_stats[location]['count'] * 100
                )
                inborn_location_stats[location]['within_48h_percentage'] = (
                    inborn_location_stats[location]['within_48h_count'] / inborn_location_stats[location]['count'] * 100
                )

    return {
        'total_babies_with_kmc': total_babies,
        'avg_time_to_initiation_hours': avg_time,
        'within_24h_count': within_24h,
        'within_24h_percentage': (within_24h / total_babies * 100),
        'within_48h_count': within_48h,
        'within_48h_percentage': (within_48h / total_babies * 100),
        'initiation_data': initiation_data,
        'inborn_stats': inborn_stats,
        'outborn_stats': outborn_stats,
        'inborn_location_stats': inborn_location_stats
    }

def calculate_average_kmc_by_location(baby_data, start_date, end_date):
    """Calculate average KMC hours by location and hospital for time period"""
    location_hospital_data = {}
    
    for baby in baby_data:
        hospital = baby.get('hospitalName', 'Unknown')
        location = baby.get('currentLocationOfTheBaby', 'Unknown')
        birth_date = convert_unix_to_datetime(baby.get('dateOfBirth'))
        
        if not birth_date:
            continue
            
        key = f"{hospital}-{location}"
        if key not in location_hospital_data:
            location_hospital_data[key] = {
                'hospital': hospital,
                'location': location,
                'total_kmc_minutes': 0,
                'observation_days': 0,
                'baby_count': 0
            }
        
        baby_has_kmc_in_period = False
        
        for obs_day in baby.get('observationDay', []):
            if obs_day.get('ageDay') is None:
                continue
                
            obs_date = birth_date.date() + timedelta(days=obs_day.get('ageDay', 0))
            
            if start_date <= obs_date <= end_date:
                kmc_minutes = obs_day.get('totalKMCtimeDay', 0)
                if kmc_minutes > 0:
                    location_hospital_data[key]['total_kmc_minutes'] += kmc_minutes
                    location_hospital_data[key]['observation_days'] += 1
                    baby_has_kmc_in_period = True
        
        if baby_has_kmc_in_period:
            location_hospital_data[key]['baby_count'] += 1
    
    # Calculate averages
    result_data = []
    for key, data in location_hospital_data.items():
        if data['observation_days'] > 0:
            avg_hours_per_day = data['total_kmc_minutes'] / data['observation_days'] / 60
            avg_hours_per_baby = data['total_kmc_minutes'] / data['baby_count'] / 60 if data['baby_count'] > 0 else 0
            
            result_data.append({
                'hospital': data['hospital'],
                'location': data['location'],
                'avg_hours_per_day': avg_hours_per_day,
                'avg_hours_per_baby': avg_hours_per_baby,
                'baby_count': data['baby_count'],
                'observation_days': data['observation_days']
            })
    
    return result_data

def calculate_critical_reason_classification(discharge_data):
    """Classify babies based on criticalReason field from discharge collection only"""

    # Process discharge collection criticalReasons only
    discharge_processed_uids = set()
    discharge_critical_reasons = {}

    for discharge in discharge_data:
        uid = discharge.get('UID')
        if not uid or uid in discharge_processed_uids:
            continue
        discharge_processed_uids.add(uid)

        critical_reasons_field = discharge.get('criticalReasons', '')
        # Only process entries that have actual critical reasons (ignore empty/null values)
        if isinstance(critical_reasons_field, str) and critical_reasons_field.strip():
            critical_reason = critical_reasons_field.strip()

            if critical_reason not in discharge_critical_reasons:
                discharge_critical_reasons[critical_reason] = {
                    'count': 0,
                    'discharges': []
                }

            discharge_critical_reasons[critical_reason]['count'] += 1
            discharge_critical_reasons[critical_reason]['discharges'].append({
                'UID': uid,
                'hospitalName': discharge.get('hospitalName', 'Unknown'),
                'criticalReason': critical_reason,
                'dischargeType': discharge.get('dischargeType', 'Unknown'),
                'dischargeStatus': discharge.get('dischargeStatus', 'Unknown'),
                'source': 'discharges collection'
            })

    return {
        'discharge_critical_reasons': discharge_critical_reasons,
        'total_discharges_with_reasons': sum(data['count'] for data in discharge_critical_reasons.values()),
        'total_discharges': len(discharge_processed_uids)
    }

def calculate_kmc_verification_monitoring(baby_data):
    """Calculate KMC verification monitoring with total numbers"""
    processed_uids = set()

    verification_stats = {
        'correct': 0,
        'incorrect': 0,
        'unable_to_verify': 0,
        'not_verified': 0,
        'total_observations': 0
    }

    detailed_data = []

    for baby in baby_data:
        uid = baby.get('UID')
        if not uid or uid in processed_uids:
            continue
        processed_uids.add(uid)

        observation_days = baby.get('observationDay', [])

        for obs_day in observation_days:
            verification_stats['total_observations'] += 1

            # Check KMC verification fields with true/false logic
            filled_correctly = obs_day.get('filledCorrectly')
            kmc_filled_correctly = obs_day.get('kmcfilledcorrectly')
            mne_comment = obs_day.get('mnecomment', '')

            status = 'not_verified'  # Default

            # Priority logic:
            # 1. If mnecomment exists, it's incorrect
            # 2. Check boolean values for verification status
            if mne_comment and mne_comment.strip():
                status = 'incorrect'
            elif filled_correctly is True:
                status = 'correct'
            elif filled_correctly is False:
                status = 'incorrect'
            elif kmc_filled_correctly is True:
                status = 'correct'
            elif kmc_filled_correctly is False:
                status = 'incorrect'
            # If string-based values still exist, handle them as fallback
            elif isinstance(kmc_filled_correctly, str) and kmc_filled_correctly:
                kmc_lower = kmc_filled_correctly.lower()
                if kmc_lower == 'correct' or kmc_lower == 'true':
                    status = 'correct'
                elif kmc_lower == 'incorrect' or kmc_lower == 'false':
                    status = 'incorrect'
                elif 'unable' in kmc_lower:
                    status = 'unable_to_verify'

            verification_stats[status] += 1

            detailed_data.append({
                'UID': uid,
                'hospitalName': baby.get('hospitalName', 'Unknown'),
                'observationDate': obs_day.get('date', 'Unknown'),
                'ageDay': obs_day.get('ageDay', 'Unknown'),
                'status': status,
                'filledCorrectly': filled_correctly,
                'kmcfilledcorrectly': kmc_filled_correctly,
                'mnecomment': mne_comment,
                'observation_data': {k: v for k, v in obs_day.items() if k not in ['filledCorrectly', 'kmcfilledcorrectly', 'mnecomment', 'date', 'ageDay']}
            })

    return {
        'verification_stats': verification_stats,
        'detailed_data': detailed_data,
        'total_babies': len(processed_uids)
    }

def calculate_observations_verification_monitoring(baby_data):
    """Calculate observations verification monitoring with total numbers"""
    processed_uids = set()

    verification_stats = {
        'correct_or_not_checked': 0,
        'incorrect': 0,
        'total_observations': 0
    }

    detailed_data = []

    for baby in baby_data:
        uid = baby.get('UID')
        if not uid or uid in processed_uids:
            continue
        processed_uids.add(uid)

        observation_days = baby.get('observationDay', [])

        for obs_day in observation_days:
            verification_stats['total_observations'] += 1

            # Check observations verification fields with true/false logic
            filled_incorrectly = obs_day.get('filledincorrectly')
            mne_comment = obs_day.get('mnecomment', '')

            status = 'correct_or_not_checked'  # Default

            # Logic: if comment there, then it wasn't correct
            # Use boolean true/false values for filledincorrectly
            if mne_comment and mne_comment.strip():
                status = 'incorrect'
            elif filled_incorrectly is True:
                status = 'incorrect'

            verification_stats[status] += 1

            detailed_data.append({
                'UID': uid,
                'hospitalName': baby.get('hospitalName', 'Unknown'),
                'observationDate': obs_day.get('date', 'Unknown'),
                'ageDay': obs_day.get('ageDay', 'Unknown'),
                'status': status,
                'filledincorrectly': filled_incorrectly,
                'mnecomment': mne_comment,
                'observation_data': {k: v for k, v in obs_day.items() if k not in ['filledincorrectly', 'mnecomment', 'date', 'ageDay']}
            })

    return {
        'verification_stats': verification_stats,
        'detailed_data': detailed_data,
        'total_babies': len(processed_uids)
    }

def clean_emoji_text(text):
    """Remove emojis from text for better processing"""
    import re
    # Remove emoji characters (basic Unicode ranges for emojis)
    emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002702-\U000027B0"  # dingbats
        u"\U000024C2-\U0001F251"
        "]+", flags=re.UNICODE)
    return emoji_pattern.sub('', text).strip()

def categorize_discharge_from_collection(record, source):
    """Categorize discharge based on collection source with user's exact rules"""

    if source == 'discharges':
        # From discharges collection, use dischargeStatus and dischargeType
        discharge_status = record.get('dischargeStatus', '').lower()
        discharge_type = record.get('dischargeType', '').lower()

        # Critical and sent home: dischargeStatus = critical and dischargeType = home
        if discharge_status == 'critical' and discharge_type == 'home':
            return 'critical_home'

        # Stable and sent home: dischargeStatus = stable and dischargeType = home
        elif discharge_status == 'stable' and discharge_type == 'home':
            return 'stable_home'

        # Critical and referred: dischargeStatus = critical and dischargeType = referred
        elif discharge_status == 'critical' and discharge_type == 'referred':
            return 'critical_referred'

        # Died: dischargeType = died
        elif discharge_type == 'died':
            return 'died'
        else:
            return 'other'

    elif source == 'babyBackUp':
        # From babybackup collection, use dischargedStatusString
        discharge_status_string = record.get('dischargedStatusString', '')
        if not discharge_status_string:
            discharge_status_string = ''

        # Clean emoji characters from the string for better matching
        cleaned_string = clean_emoji_text(discharge_status_string)
        discharge_status_lower = discharge_status_string.lower()
        cleaned_lower = cleaned_string.lower()

        # Critical and sent home: "Critical and discharged"
        if ('critical and discharged' in discharge_status_lower):
            return 'critical_home'

        # Stable and sent home: "Discharged according to criteria/stable"
        elif ('discharged according to criteria' in discharge_status_lower or
              'stable' in discharge_status_lower):
            return 'stable_home'

        # Critical and referred: "Referred out/Critical"
        elif ('referred out' in discharge_status_lower or
              'critical' in discharge_status_lower):
            return 'critical_referred'

        # Died: "डिस्चार्ज से पहले ही मृत्यु हो गई 👼" or "died before discharge"
        elif ('डिस्चार्ज से पहले ही मृत्यु हो गई' in discharge_status_string or
              'died before discharge' in discharge_status_lower or
              'मृत्यु हो गई' in discharge_status_string or
              'death' in discharge_status_lower):
            return 'died'
        else:
            return 'other'

    return 'other'

def calculate_discharge_outcomes(baby_data, discharge_data):
    """Calculate discharge outcomes using ONLY discharges and babyBackUp collections"""

    # Initialize categories
    discharge_categories = {
        'critical_home': {'count': 0, 'babies': []},
        'stable_home': {'count': 0, 'babies': []},
        'critical_referred': {'count': 0, 'babies': []},
        'died': {'count': 0, 'babies': []},
        'other': {'count': 0, 'babies': []}
    }

    # Track processed UIDs to avoid duplicates
    processed_uids = set()

    # Process discharge collection first
    for discharge in discharge_data:
        uid = discharge.get('UID')
        if not uid or uid in processed_uids:
            continue

        processed_uids.add(uid)
        category = categorize_discharge_from_collection(discharge, 'discharges')

        # Add to appropriate category
        discharge_categories[category]['count'] += 1
        discharge_categories[category]['babies'].append({
            'UID': uid,
            'hospitalName': discharge.get('hospitalName', 'Unknown'),
            'dischargeStatus': discharge.get('dischargeStatus', 'N/A'),
            'dischargeType': discharge.get('dischargeType', 'N/A'),
            'source': 'discharges collection',
            'discharge_record': True,
            'baby_data': discharge
        })

    # Process ONLY babyBackUp collection data (filter by source)
    babybackup_data = [baby for baby in baby_data if baby.get('source') == 'babyBackUp']
    for baby in babybackup_data:
        uid = baby.get('UID')

        if not uid or uid in processed_uids:
            continue

        processed_uids.add(uid)
        category = categorize_discharge_from_collection(baby, 'babyBackUp')

        # Add to appropriate category
        discharge_categories[category]['count'] += 1
        discharge_categories[category]['babies'].append({
            'UID': uid,
            'hospitalName': baby.get('hospitalName', 'Unknown'),
            'dischargeStatusString': baby.get('dischargeStatusString', 'Unknown'),
            'source': 'babyBackUp collection',
            'discharge_record': False,
            'baby_data': baby
        })

    total_discharged = sum(cat['count'] for cat in discharge_categories.values())
    
    return {
        'categories': discharge_categories,
        'total_discharged': total_discharged,
        'unique_babies_processed': len(processed_uids),
        'critical_home_percentage': (discharge_categories['critical_home']['count'] / total_discharged * 100) if total_discharged > 0 else 0,
        'stable_home_percentage': (discharge_categories['stable_home']['count'] / total_discharged * 100) if total_discharged > 0 else 0,
        'critical_referred_percentage': (discharge_categories['critical_referred']['count'] / total_discharged * 100) if total_discharged > 0 else 0,
        'died_percentage': (discharge_categories['died']['count'] / total_discharged * 100) if total_discharged > 0 else 0
    }

def calculate_individual_critical_reasons(discharge_data):
    """Calculate individual critical reasons from discharge collection - parse array-like strings"""
    import ast
    import re

    # Track individual critical reasons
    individual_reasons = {}
    total_babies_with_reasons = 0
    processed_uids = set()

    for discharge in discharge_data:
        uid = discharge.get('UID')
        if not uid or uid in processed_uids:
            continue

        critical_reasons_field = discharge.get('criticalReasons', '')

        # Only process entries that have actual critical reasons data
        if not critical_reasons_field or not str(critical_reasons_field).strip():
            continue

        processed_uids.add(uid)
        total_babies_with_reasons += 1

        try:
            # Parse the string representation of array (e.g., "['GA', 'weightLoss>2%']")
            critical_reasons_str = str(critical_reasons_field).strip()

            # Handle different formats
            if critical_reasons_str.startswith('[') and critical_reasons_str.endswith(']'):
                # Try to parse as Python list literal
                try:
                    reasons_list = ast.literal_eval(critical_reasons_str)
                except:
                    # Fallback: extract items using regex
                    reasons_list = re.findall(r"'([^']*)'", critical_reasons_str)
            else:
                # Single reason, not in array format
                reasons_list = [critical_reasons_str]

            # Count each individual reason
            for reason in reasons_list:
                reason = str(reason).strip()
                if reason:
                    if reason not in individual_reasons:
                        individual_reasons[reason] = {
                            'count': 0,
                            'babies': []
                        }

                    individual_reasons[reason]['count'] += 1
                    individual_reasons[reason]['babies'].append({
                        'UID': uid,
                        'hospital': discharge.get('hospitalName', 'Unknown'),
                        'dischargeStatus': discharge.get('dischargeStatus', 'Unknown'),
                        'full_reasons': critical_reasons_str
                    })

        except Exception as e:
            # If parsing fails, treat as single reason
            reason = str(critical_reasons_field).strip()
            if reason:
                if reason not in individual_reasons:
                    individual_reasons[reason] = {
                        'count': 0,
                        'babies': []
                    }

                individual_reasons[reason]['count'] += 1
                individual_reasons[reason]['babies'].append({
                    'UID': uid,
                    'hospital': discharge.get('hospitalName', 'Unknown'),
                    'dischargeStatus': discharge.get('dischargeStatus', 'Unknown'),
                    'full_reasons': critical_reasons_field
                })

    return {
        'individual_reasons': individual_reasons,
        'total_babies_with_reasons': total_babies_with_reasons,
        'total_unique_reasons': len(individual_reasons)
    }

def calculate_followup_metrics(followup_data, baby_data):
    """Calculate follow-up completion metrics from baby/babybackup collections only (NOT follow_up collection)"""
    
    # Track processed UIDs to avoid duplicates
    processed_uids = set()
    followup_summary = []
    
    # Define follow-up requirements with followUpNumber mappings
    followup_requirements = {
        'Follow up 2': {'days_from_discharge': 3, 'followup_number': 2},
        'Follow up 7': {'days_from_discharge': 7, 'followup_number': 7}, 
        'Follow up 14': {'days_from_discharge': 14, 'followup_number': 14},
        'Follow up 28': {'days_from_birth': 29, 'followup_number': 28}
    }
    
    hospital_stats = {}
    
    for baby in baby_data:
        uid = baby.get('UID')
        if not uid or uid in processed_uids:
            continue
        
        # Filter out dead babies
        if baby.get('deadBaby') == True:
            continue
            
        processed_uids.add(uid)
        
        hospital = baby.get('hospitalName', 'Unknown')
        birth_date = convert_unix_to_datetime(baby.get('dateOfBirth'))
        
        # Get discharge date - check multiple possible fields
        discharge_date = None
        if baby.get('lastDischargeType') and baby.get('lastDischargeType').lower() != 'died':
            discharge_date = convert_unix_to_datetime(baby.get('dischargeDate') or 
                                                    baby.get('lastDischargeDate') or
                                                    baby.get('actualDischargeDate'))
        
        if hospital not in hospital_stats:
            hospital_stats[hospital] = {}
            for followup_name in followup_requirements.keys():
                hospital_stats[hospital][followup_name] = {
                    'eligible': 0,
                    'completed': 0,
                    'due': 0,
                    'overdue': 0
                }
        
        # Check each follow-up requirement
        for followup_name, req in followup_requirements.items():
            followup_number = req['followup_number']
            
            # Determine if baby is eligible for this follow-up
            eligible = False
            due_date = None
            
            if followup_name == 'Follow up 28':
                # Follow up 28: due by birth date + 29 days
                if birth_date:
                    due_date = birth_date.date() + timedelta(days=req['days_from_birth'])
                    eligible = True
            else:
                # Follow ups 2, 7, 14: due by discharge date + X days
                if discharge_date:
                    due_date = discharge_date.date() + timedelta(days=req['days_from_discharge'])
                    eligible = True
            
            if eligible:
                hospital_stats[hospital][followup_name]['eligible'] += 1
                
                # Check if follow-up is completed by looking at followUp array
                followup_completed = False
                followup_array = baby.get('followUp', [])
                
                # Check if this followup number exists in the followUp array
                for followup_entry in followup_array:
                    if followup_entry.get('followUpNumber') == followup_number:
                        followup_completed = True
                        break
                
                if followup_completed:
                    hospital_stats[hospital][followup_name]['completed'] += 1
                else:
                    # Check if it's overdue (only consider follow-ups due until yesterday)
                    yesterday = datetime.now().date() - timedelta(days=1)
                    if due_date and due_date <= yesterday:
                        hospital_stats[hospital][followup_name]['overdue'] += 1
                    # Note: We don't count future due dates as they're not yet actionable
    
    # Convert to summary format
    for hospital, followups in hospital_stats.items():
        for followup_name, stats in followups.items():
            if stats['eligible'] > 0:
                completion_rate = (stats['completed'] / stats['eligible']) * 100
                followup_summary.append({
                    'hospital': hospital,
                    'followup_type': followup_name,
                    'eligible': stats['eligible'],
                    'completed': stats['completed'],
                    'completion_rate': completion_rate,
                    'due': stats['due'],
                    'overdue': stats['overdue']
                })
    
    # Calculate overall stats
    total_eligible = sum(item['eligible'] for item in followup_summary)
    total_completed = sum(item['completed'] for item in followup_summary)
    overall_completion_rate = (total_completed / total_eligible * 100) if total_eligible > 0 else 0
    
    return {
        'followup_types': list(followup_requirements.keys()),
        'total_eligible': total_eligible,
        'total_completed': total_completed,
        'overall_completion_rate': overall_completion_rate,
        'hospital_summary': followup_summary,
        'unique_babies_processed': len(processed_uids)
    }

def calculate_hospital_stay_duration(baby_data):
    """Calculate average hospital stay duration by location, formatted as 'y days x hours'"""
    processed_uids = set()
    stay_data = []

    for baby in baby_data:
        uid = baby.get('UID')
        if not uid or uid in processed_uids:
            continue
        processed_uids.add(uid)

        # Get birth date
        birth_date = convert_unix_to_datetime(baby.get('dateOfBirth'))
        if not birth_date:
            continue

        # Get discharge date based on source
        discharge_date = None
        source = baby.get('source', '')

        if source == 'baby':
            # For baby collection, use lastDischargeDate
            if baby.get('lastDischargeDate'):
                discharge_date = convert_unix_to_datetime(baby.get('lastDischargeDate'))
        elif source == 'babyBackUp':
            # For babyBackUp collection, use dischargeDate
            if baby.get('dischargeDate'):
                discharge_date = convert_unix_to_datetime(baby.get('dischargeDate'))

        if discharge_date and discharge_date > birth_date:
            # Calculate stay duration in days
            stay_duration = (discharge_date - birth_date).total_seconds() / (24 * 3600)  # Convert to days

            location = baby.get('currentLocationOfTheBaby', 'Unknown')
            hospital = baby.get('hospitalName', 'Unknown')

            stay_data.append({
                'UID': uid,
                'hospital': hospital,
                'location': location,
                'stay_duration_days': stay_duration,
                'birth_date': birth_date,
                'discharge_date': discharge_date,
                'source': source
            })

    # Group by location
    location_stats = {}
    for record in stay_data:
        location = record['location']
        if location not in location_stats:
            location_stats[location] = {
                'durations': [],
                'count': 0,
                'total_days': 0,
                'avg_days': 0,
                'avg_formatted': '0 days 0 hours'
            }

        location_stats[location]['durations'].append(record['stay_duration_days'])
        location_stats[location]['count'] += 1
        location_stats[location]['total_days'] += record['stay_duration_days']

    # Calculate averages and format
    for location, stats in location_stats.items():
        if stats['count'] > 0:
            avg_days_float = stats['total_days'] / stats['count']
            days = int(avg_days_float)
            hours = int((avg_days_float - days) * 24)

            stats['avg_days'] = avg_days_float
            stats['avg_formatted'] = f"{days} days {hours} hours"

    return {
        'location_stats': location_stats,
        'raw_data': stay_data,
        'total_babies': len(stay_data)
    }

def calculate_individual_baby_metrics(baby_data):
    """Calculate comprehensive metrics for each individual baby"""
    processed_uids = set()
    baby_metrics = []

    for baby in baby_data:
        uid = baby.get('UID')
        if not uid or uid in processed_uids:
            continue
        processed_uids.add(uid)

        # Basic baby info
        mother_name = baby.get('motherName', 'Unknown')
        hospital = baby.get('hospitalName', 'Unknown')
        location = baby.get('currentLocationOfTheBaby', 'Unknown')
        dead_baby = baby.get('deadBaby', False)
        danger_signs = baby.get('dangerSigns', 'Not specified')

        # Calculate KMC metrics
        total_kmc_minutes = 0
        kmc_days_count = 0
        observation_days = baby.get('observationDay', [])

        # Calculate total KMC and average per day
        for obs_day in observation_days:
            kmc_time = obs_day.get('totalKMCtimeDay', 0)
            if kmc_time > 0:
                total_kmc_minutes += kmc_time
                kmc_days_count += 1

        total_kmc_hours = total_kmc_minutes / 60 if total_kmc_minutes > 0 else 0
        avg_kmc_per_day = total_kmc_hours / kmc_days_count if kmc_days_count > 0 else 0

        # Calculate follow-up KMC averages for specific follow-up numbers
        followup_kmc = {2: [], 7: [], 14: [], 28: []}

        followup_array = baby.get('followUp', [])
        for followup_entry in followup_array:
            followup_number = followup_entry.get('followUpNumber')
            if followup_number in followup_kmc:
                kmc_time = followup_entry.get('totalKMCTime')
                if kmc_time is not None:
                    try:
                        kmc_hours = float(kmc_time) / 60 if float(kmc_time) > 0 else 0
                        followup_kmc[followup_number].append(kmc_hours)
                    except (ValueError, TypeError):
                        pass

        # Calculate averages for each follow-up
        followup_averages = {}
        for followup_num, times in followup_kmc.items():
            if times:
                followup_averages[f'Follow-up {followup_num}'] = f"{sum(times)/len(times):.1f}h"
            else:
                followup_averages[f'Follow-up {followup_num}'] = "No data"

        baby_metrics.append({
            'UID': uid,
            'Mother Name': mother_name,
            'Hospital': hospital,
            'Location': location,
            'Total KMC Hours': f"{total_kmc_hours:.1f}h",
            'Avg KMC Hours/Day': f"{avg_kmc_per_day:.1f}h",
            'KMC Days Count': kmc_days_count,
            'Follow-up 2': followup_averages['Follow-up 2'],
            'Follow-up 7': followup_averages['Follow-up 7'],
            'Follow-up 14': followup_averages['Follow-up 14'],
            'Follow-up 28': followup_averages['Follow-up 28'],
            'Dead Baby': 'Yes' if dead_baby else 'No',
            'Danger Signs': danger_signs,
            'Birth Date': convert_unix_to_datetime(baby.get('dateOfBirth')),
            'Source': baby.get('source', 'Unknown')
        })

    return baby_metrics

def calculate_skin_contact_metrics(baby_data):
    """Calculate average numberSkinContact from all followups EXCEPT followUp28 in baby/babybackup collections"""
    processed_uids = set()
    skin_contact_data = []
    high_skin_contact_alerts = []

    for baby in baby_data:
        uid = baby.get('UID')
        if not uid or uid in processed_uids:
            continue
        processed_uids.add(uid)

        # Look for all followups EXCEPT followUp28 in the followUp array
        followup_array = baby.get('followUp', [])
        for followup_entry in followup_array:
            followup_number = followup_entry.get('followUpNumber')

            # Skip followUp28 as requested
            if followup_number == 28:
                continue

            number_skin_contact = followup_entry.get('numberSkinContact')
            if number_skin_contact is not None:
                try:
                    skin_contact_value = float(number_skin_contact)

                    skin_contact_data.append({
                        'UID': uid,
                        'hospital': baby.get('hospitalName', 'Unknown'),
                        'numberSkinContact': skin_contact_value,
                        'followUpNumber': followup_number
                    })

                    # Alert for skin-to-skin contact > 10
                    if skin_contact_value > 10:
                        high_skin_contact_alerts.append({
                            'UID': uid,
                            'hospital': baby.get('hospitalName', 'Unknown'),
                            'numberSkinContact': skin_contact_value,
                            'followUpNumber': followup_number
                        })

                except (ValueError, TypeError):
                    pass  # Skip invalid values
    
    if not skin_contact_data:
        return {
            'total_babies_with_data': 0,
            'average_skin_contact': 0,
            'min_skin_contact': 0,
            'max_skin_contact': 0,
            'skin_contact_data': [],
            'high_skin_contact_alerts': []
        }

    values = [item['numberSkinContact'] for item in skin_contact_data]

    return {
        'total_babies_with_data': len(skin_contact_data),
        'average_skin_contact': sum(values) / len(values),
        'min_skin_contact': min(values),
        'max_skin_contact': max(values),
        'skin_contact_data': skin_contact_data,
        'high_skin_contact_alerts': high_skin_contact_alerts
    }

def analyze_kmc_filled_correctly(baby_data):
    """Analyze KMCfilledcorrectlystring categorization"""
    processed_uids = set()
    kmc_filled_data = {
        'correct': [],
        'incorrect': [],
        'missing': []
    }
    
    for baby in baby_data:
        uid = baby.get('UID')
        if not uid or uid in processed_uids:
            continue
        processed_uids.add(uid)
        
        # Check KMCfilledcorrectlystring in observationDay
        for obs_day in baby.get('observationDay', []):
            kmc_filled_string = obs_day.get('KMCfilledcorrectlystring', '').lower()
            kmc_hours = obs_day.get('totalKMCtimeDay', 0) / 60 if obs_day.get('totalKMCtimeDay') else 0
            age_day = obs_day.get('ageDay', 'Unknown')
            me_comment = obs_day.get('MEComment', 'No comment')
            
            entry_data = {
                'UID': uid,
                'hospital': baby.get('hospitalName', 'Unknown'),
                'ageDay': age_day,
                'KMChours': round(kmc_hours, 1),
                'MEComment': me_comment,
                'KMCfilledcorrectlystring': obs_day.get('KMCfilledcorrectlystring', 'Missing'),
                'baby_data': baby
            }
            
            if not kmc_filled_string:
                kmc_filled_data['missing'].append(entry_data)
            elif 'correct' in kmc_filled_string or 'true' in kmc_filled_string:
                kmc_filled_data['correct'].append(entry_data)
            elif 'incorrect' in kmc_filled_string or 'false' in kmc_filled_string:
                kmc_filled_data['incorrect'].append(entry_data)
            else:
                kmc_filled_data['incorrect'].append(entry_data)  # Default unclear to incorrect
    
    return kmc_filled_data

def analyze_observation_filled_correctly(baby_data):
    """Analyze observation day filledcorrectly field"""
    processed_uids = set()
    obs_filled_data = {
        'correct': [],
        'incorrect': [],
        'missing': []
    }
    
    for baby in baby_data:
        uid = baby.get('UID')
        if not uid or uid in processed_uids:
            continue
        processed_uids.add(uid)
        
        # Check filledcorrectly in observationDay
        for obs_day in baby.get('observationDay', []):
            filled_correctly = obs_day.get('filledcorrectly')
            kmc_hours = obs_day.get('totalKMCtimeDay', 0) / 60 if obs_day.get('totalKMCtimeDay') else 0
            age_day = obs_day.get('ageDay', 'Unknown')
            me_comment = obs_day.get('MEComment', 'No comment')
            
            entry_data = {
                'UID': uid,
                'hospital': baby.get('hospitalName', 'Unknown'),
                'ageDay': age_day,
                'KMChours': round(kmc_hours, 1),
                'MEComment': me_comment,
                'filledcorrectly': filled_correctly,
                'baby_data': baby
            }
            
            if filled_correctly is None:
                obs_filled_data['missing'].append(entry_data)
            elif filled_correctly == True:
                obs_filled_data['correct'].append(entry_data)
            elif filled_correctly == False:
                obs_filled_data['incorrect'].append(entry_data)
            else:
                obs_filled_data['missing'].append(entry_data)
    
    return obs_filled_data

def find_high_kmc_followups(baby_data):
    """Find follow-ups with KMC hours >12 per day including nurse name"""
    high_kmc_data = []
    processed_uids = set()
    
    for baby in baby_data:
        uid = baby.get('UID')
        if not uid or uid in processed_uids:
            continue
        processed_uids.add(uid)
        
        # Check follow-up entries
        followup_array = baby.get('followUp', [])
        for followup_entry in followup_array:
            # Check if there are KMC hours data
            kmc_hours = followup_entry.get('kmcHours', 0)
            if kmc_hours > 12:  # More than 12 hours per day
                high_kmc_data.append({
                    'UID': uid,
                    'hospital': baby.get('hospitalName', 'Unknown'),
                    'followUpNumber': followup_entry.get('followUpNumber', 'Unknown'),
                    'KMChours': kmc_hours,
                    'nurseName': followup_entry.get('nurseName', baby.get('nurseName', 'Not specified')),
                    'followUpDate': followup_entry.get('date', 'Unknown'),
                    'dataset': baby.get('source', 'baby'),
                    'baby_data': baby
                })
    
    return high_kmc_data

def analyze_kmc_filled_comparison(baby_data):
    """Compare kmcFilledCorrectlyString = 'correct' vs KMCfilledCorrectly = false"""
    comparison_data = {
        'string_correct': [],
        'boolean_false': [],
        'both_mismatch': []
    }
    processed_uids = set()
    
    for baby in baby_data:
        uid = baby.get('UID')
        if not uid or uid in processed_uids:
            continue
        processed_uids.add(uid)
        
        # Check observation days
        for obs_day in baby.get('observationDay', []):
            kmc_filled_string = obs_day.get('KMCfilledcorrectlystring', '').lower()
            kmc_filled_correctly = obs_day.get('KMCfilledCorrectly')  # Note the capital C
            kmc_hours = obs_day.get('totalKMCtimeDay', 0) / 60 if obs_day.get('totalKMCtimeDay') else 0
            age_day = obs_day.get('ageDay', 'Unknown')
            me_comment = obs_day.get('MEComment', 'No comment')
            
            entry_data = {
                'UID': uid,
                'hospital': baby.get('hospitalName', 'Unknown'),
                'ageDay': age_day,
                'KMChours': round(kmc_hours, 1),
                'MEComment': me_comment,
                'KMCfilledcorrectlystring': obs_day.get('KMCfilledcorrectlystring', 'Missing'),
                'KMCfilledCorrectly': kmc_filled_correctly,
                'baby_data': baby
            }
            
            # Check for kmcFilledCorrectlyString = "correct"
            if 'correct' in kmc_filled_string:
                comparison_data['string_correct'].append(entry_data)
            
            # Check for KMCfilledCorrectly = false
            if kmc_filled_correctly == False:
                comparison_data['boolean_false'].append(entry_data)
                
            # Check for mismatch (string says correct but boolean is false)
            if 'correct' in kmc_filled_string and kmc_filled_correctly == False:
                comparison_data['both_mismatch'].append(entry_data)
    
    return comparison_data

def check_kmc_stability(baby):
    """Check if baby is unstable for KMC based on updated criteria"""
    has_kmc_hours = False
    is_unstable = False
    total_kmc_time = 0
    
    for obs_day in baby.get('observationDay', []):
        # Check for KMC hours
        kmc_time = obs_day.get('totalKMCtimeDay', 0)
        if kmc_time > 0:
            has_kmc_hours = True
            total_kmc_time += kmc_time
        
        # Check for unstable indicators
        if obs_day.get('unstableForKMC') == True:
            is_unstable = True
        
        # Check danger sign for KMC instability
        danger_sign = obs_day.get('dangerSign', '')
        if 'केएमसी के लिए अस्थिर 🦘🚫' in str(danger_sign):
            is_unstable = True
    
    # Updated logic: Consider unstable if:
    # 1. Has explicit unstable indicators, OR
    # 2. Has zero KMC hours (regardless of indicators)
    if is_unstable or total_kmc_time == 0:
        return 'unstable'
    else:
        return 'stable'

def calculate_death_rates(baby_data, discharge_data):
    """Calculate comprehensive death rate KPIs using both baby and babybackup collections with deadBaby = true check"""
    
    # Track processed UIDs to avoid duplicates
    processed_uids = set()
    
    # Initialize categories for discharge analysis (only for dead babies)
    discharge_categories = {
        'critical_home': {'count': 0, 'babies': []},
        'stable_home': {'count': 0, 'babies': []},
        'critical_referred': {'count': 0, 'babies': []},
        'died': {'count': 0, 'babies': []},
        'other': {'count': 0, 'babies': []}
    }
    
    # Calculate by hospital
    hospital_deaths = {}
    hospital_totals = {}
    
    # Inborn vs Outborn analysis
    inborn_total = 0
    inborn_deaths = 0
    outborn_total = 0
    outborn_deaths = 0
    
    # Location analysis
    location_analysis = {}
    
    # KMC stability analysis with updated criteria
    kmc_stability = {'stable': {'total': 0, 'deaths': 0}, 'unstable': {'total': 0, 'deaths': 0}}
    
    # Process ALL baby data (both baby and babyBackUp collections)
    for baby in baby_data:
        uid = baby.get('UID')
        if not uid or uid in processed_uids:
            continue
        processed_uids.add(uid)
        
        hospital = baby.get('hospitalName', 'Unknown')
        
        # Check if baby is dead using deadBaby = true field
        is_dead = baby.get('deadBaby') == True
        
        # Hospital analysis
        if hospital not in hospital_totals:
            hospital_totals[hospital] = 0
            hospital_deaths[hospital] = 0
        hospital_totals[hospital] += 1
        if is_dead:
            hospital_deaths[hospital] += 1
        
        # Inborn vs Outborn
        place_of_delivery = baby.get('placeOfDelivery', '')
        if place_of_delivery in ['यह अस्पताल', 'this hospital']:
            inborn_total += 1
            if is_dead:
                inborn_deaths += 1
        else:
            outborn_total += 1
            if is_dead:
                outborn_deaths += 1
        
        # Location analysis
        location = baby.get('currentLocationOfTheBaby', 'Unknown')
        if location not in location_analysis:
            location_analysis[location] = {'total': 0, 'deaths': 0}
        location_analysis[location]['total'] += 1
        if is_dead:
            location_analysis[location]['deaths'] += 1
        
        # KMC Stability Analysis with updated criteria
        stability = check_kmc_stability(baby)
        kmc_stability[stability]['total'] += 1
        if is_dead:
            kmc_stability[stability]['deaths'] += 1
        
        # Discharge categorization ONLY for dead babies
        if is_dead:
            category = 'other'
            
            # Check if baby is from discharge collection
            matching_discharge = None
            for discharge in discharge_data:
                if discharge.get('UID') == uid:
                    matching_discharge = discharge
                    break
            
            if matching_discharge:
                category = categorize_discharge_from_collection(matching_discharge, 'discharges')
            elif baby.get('source') == 'babyBackUp':
                category = categorize_discharge_from_collection(baby, 'babyBackUp')
            
            # Add to appropriate category
            discharge_categories[category]['count'] += 1
            discharge_categories[category]['babies'].append({
                'UID': uid,
                'hospitalName': baby.get('hospitalName', 'Unknown'),
                'dischargeStatusString': baby.get('dischargeStatusString', 'Unknown'),
                'source': baby.get('source', 'Unknown'),
                'baby_data': baby
            })
    
    total_babies = len(processed_uids)
    dead_babies = sum(1 for baby in baby_data if baby.get('deadBaby') == True)
    
    # Create discharge status summary using the categories (only for dead babies)
    discharge_status = {
        'Critical and sent home': {'total': discharge_categories['critical_home']['count'], 'deaths': discharge_categories['critical_home']['count']},
        'Stable and sent home': {'total': discharge_categories['stable_home']['count'], 'deaths': discharge_categories['stable_home']['count']},
        'Critical and referred': {'total': discharge_categories['critical_referred']['count'], 'deaths': discharge_categories['critical_referred']['count']},
        'Died': {'total': discharge_categories['died']['count'], 'deaths': discharge_categories['died']['count']},
        'Other/Unknown': {'total': discharge_categories['other']['count'], 'deaths': discharge_categories['other']['count']}
    }
    
    # Create discharge outcomes structure for compatibility
    discharge_outcomes = {
        'categories': discharge_categories,
        'total_discharged': sum(cat['count'] for cat in discharge_categories.values()),
        'unique_babies_processed': dead_babies
    }
    
    return {
        'total_babies': total_babies,
        'dead_babies': dead_babies,
        'mortality_rate': (dead_babies / total_babies * 100) if total_babies > 0 else 0,
        'hospital_data': {
            'hospitals': list(hospital_totals.keys()),
            'totals': list(hospital_totals.values()),
            'deaths': list(hospital_deaths.values()),
            'rates': [(hospital_deaths[h] / hospital_totals[h] * 100) if hospital_totals[h] > 0 else 0 
                     for h in hospital_totals.keys()]
        },
        'birth_place': {
            'inborn': {'total': inborn_total, 'deaths': inborn_deaths},
            'outborn': {'total': outborn_total, 'deaths': outborn_deaths}
        },
        'discharge_status': discharge_status,
        'discharge_outcomes': discharge_outcomes,
        'location_analysis': location_analysis,
        'kmc_stability': kmc_stability
    }

def calculate_daily_kmc_analysis(baby_data):
    """Calculate daily KMC analysis for last 3 days, excluding babies discharged on the same day"""
    today = datetime.now().date()
    analysis_data = {}
    excluded_counts = {}  # Track excluded babies per date

    # Get all hospitals and locations
    hospitals = sorted(list(set(baby.get('hospitalName') for baby in baby_data if baby.get('hospitalName'))))
    locations = sorted(list(set(baby.get('currentLocationOfTheBaby') for baby in baby_data
                        if baby.get('currentLocationOfTheBaby'))))

    # Analyze last 3 days
    for day_offset in range(1, 4):
        target_date = today - timedelta(days=day_offset)
        date_key = target_date.strftime('%Y-%m-%d')

        analysis_data[date_key] = {}
        excluded_counts[date_key] = 0

        for hospital in hospitals:
            analysis_data[date_key][hospital] = {}
            for location in locations:
                analysis_data[date_key][hospital][location] = {
                    'total_kmc_minutes': 0,
                    'baby_count': 0,
                    'average_kmc_hours': 0
                }

        # Process babies for this date
        for baby in baby_data:
            if (baby.get('hospitalName') not in hospitals or
                baby.get('currentLocationOfTheBaby') not in locations or
                not baby.get('observationDay')):
                continue

            birth_date = convert_unix_to_datetime(baby.get('dateOfBirth'))
            if not birth_date:
                continue

            # Check if baby was discharged on the target date
            discharge_date = None
            last_discharge_date = baby.get('lastDischargeDate')
            if last_discharge_date:
                discharge_date = convert_unix_to_datetime(last_discharge_date)
                if discharge_date and discharge_date.date() == target_date:
                    excluded_counts[date_key] += 1
                    continue  # Skip this baby as they were discharged on the analysis date

            # Find observation for target date
            for obs_day in baby.get('observationDay', []):
                if obs_day.get('ageDay') is None:
                    continue

                obs_date = birth_date.date() + timedelta(days=obs_day.get('ageDay', 0))

                if obs_date == target_date and obs_day.get('totalKMCtimeDay', 0) > 0:
                    hospital = baby['hospitalName']
                    location = baby['currentLocationOfTheBaby']

                    analysis_data[date_key][hospital][location]['total_kmc_minutes'] += obs_day['totalKMCtimeDay']
                    analysis_data[date_key][hospital][location]['baby_count'] += 1
        
        # Calculate averages
        for hospital in hospitals:
            for location in locations:
                data = analysis_data[date_key][hospital][location]
                if data['baby_count'] > 0:
                    data['average_kmc_hours'] = round(data['total_kmc_minutes'] / data['baby_count'] / 60, 1)
    
    return analysis_data, hospitals, locations, excluded_counts

def main():
    # Header
    st.markdown("""
    <div class="main-header">
        <h1>👶 Ansh KMC Dashboard</h1>
        <p>Real-time monitoring of Kangaroo Mother Care program across hospitals</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Load data
    baby_data, discharge_data, followup_data = load_firebase_data()
    
    if not baby_data:
        st.error("No data loaded. Please check your Firebase connection.")
        return
    
    # Sidebar filters
    st.sidebar.markdown(f"""
    <div style="background: {ANSH_COLORS['light']}; padding: 1rem; border-radius: 10px; margin-bottom: 1rem;">
        <h3 style="color: {ANSH_COLORS['dark']}; margin-top: 0;">🔍 Filters</h3>
    </div>
    """, unsafe_allow_html=True)
    
    hospitals = ['All'] + sorted(list(set(baby.get('hospitalName') for baby in baby_data if baby.get('hospitalName'))))
    selected_hospital = st.sidebar.selectbox("Hospital", hospitals)
    
    # Date range filter
    start_date = st.sidebar.date_input("From Date", datetime.now() - timedelta(days=30))
    end_date = st.sidebar.date_input("To Date", datetime.now())
    
    # UID search
    search_uid = st.sidebar.text_input("Search UID")
    
    # Apply filters
    filtered_data = baby_data.copy()
    
    if selected_hospital != 'All':
        filtered_data = [baby for baby in filtered_data if baby.get('hospitalName') == selected_hospital]
    
    if search_uid:
        filtered_data = [baby for baby in filtered_data 
                       if search_uid.lower() in baby.get('UID', '').lower()]
    
    # Date filtering
    if start_date and end_date:
        filtered_data = [baby for baby in filtered_data 
                       if baby.get('dateOfBirth') and start_date <= convert_unix_to_datetime(baby.get('dateOfBirth')).date() <= end_date]
    
    st.sidebar.success(f"Showing {len(filtered_data)} babies")
    
    # Main tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["📊 Overview", "📈 Clinical KPIs", "💀 Mortality Analysis", "⏰ Daily KMC Analysis", "📊 Monitoring", "📋 Data Explorer"])
    
    with tab1:
        st.header("Program Overview")
        
        # Basic metrics - Updated definitions
        total_babies = len(baby_data)  # All babies from both baby and babyBackUp collections
        
        # Get active babies list for discharged calculation
        active_babies_list = [baby for baby in baby_data if baby.get('babyInProgram')]
        active_babies = len(active_babies_list)  # Baby in program is true
        
        discharged_babies = len([baby for baby in active_babies_list if baby.get('discharged')])  # Discharged is true out of active babies
        hospitals_count = len(set(baby.get('hospitalName') for baby in filtered_data if baby.get('hospitalName')))
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Babies", f"{total_babies:,}")
        with col2:
            st.metric("Active Cases", f"{active_babies:,}")
        with col3:
            st.metric("Discharged", f"{discharged_babies:,}")
        with col4:
            st.metric("Hospitals", hospitals_count)
        
        # Hospital distribution
        hospital_counts = {}
        for baby in filtered_data:
            hospital = baby.get('hospitalName', 'Unknown')
            hospital_counts[hospital] = hospital_counts.get(hospital, 0) + 1
        
        if hospital_counts:
            fig = px.bar(
                x=list(hospital_counts.keys()),
                y=list(hospital_counts.values()),
                title="Baby Count by Hospital",
                color_discrete_sequence=[ANSH_COLORS['primary']]
            )
            fig.update_layout(xaxis_title="Hospital", yaxis_title="Number of Babies")
            st.plotly_chart(fig, width='stretch')
    
    with tab2:
        st.header("Clinical KPIs")
        
        # Registration Timeliness
        st.subheader("Registration Timeliness (Inborn Babies)")
        reg_metrics = calculate_registration_timeliness(filtered_data)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Inborn", f"{reg_metrics['total_inborn']} out of {len(filtered_data)} total babies")
        with col2:
            st.metric("Within 24h", f"{reg_metrics['within_24h_percentage']:.1f}%", 
                     f"{reg_metrics['within_24h_count']} babies")
        with col3:
            st.metric("Within 12h", f"{reg_metrics['within_12h_percentage']:.1f}%",
                     f"{reg_metrics['within_12h_count']} babies")
        
        # Registration pie chart
        if reg_metrics['total_inborn'] > 0:
            fig = go.Figure(data=[go.Pie(
                labels=['Within 12h', '12-24h', '>24h'],
                values=[
                    reg_metrics['within_12h_count'],
                    reg_metrics['within_24h_count'] - reg_metrics['within_12h_count'],
                    reg_metrics['total_inborn'] - reg_metrics['within_24h_count']
                ],
                marker_colors=[ANSH_COLORS['primary'], ANSH_COLORS['secondary'], '#E5E7EB']
            )])
            fig.update_layout(title="Registration Timeliness Distribution")
            st.plotly_chart(fig, width='stretch')

        # Hospital Stay Duration Analysis
        st.subheader("Average Hospital Stay Duration by Location")
        stay_duration = calculate_hospital_stay_duration(filtered_data)

        if stay_duration['total_babies'] > 0:
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Discharged Babies", stay_duration['total_babies'])

            # Display by location
            if stay_duration['location_stats']:
                location_df = []
                for location, stats in stay_duration['location_stats'].items():
                    location_df.append({
                        'Location': location,
                        'Babies': stats['count'],
                        'Average Stay': stats['avg_formatted'],
                        'Days (decimal)': f"{stats['avg_days']:.1f}"
                    })

                df_display = pd.DataFrame(location_df)
                st.dataframe(df_display, width='stretch', hide_index=True)

                # Chart showing average stay by location
                fig = px.bar(
                    df_display,
                    x='Location',
                    y='Days (decimal)',
                    title="Average Hospital Stay Duration by Location",
                    color_discrete_sequence=[ANSH_COLORS['primary']]
                )
                fig.update_layout(
                    yaxis_title="Days",
                    xaxis_title="Current Location of Baby"
                )
                st.plotly_chart(fig, width='stretch')
        else:
            st.info("No discharged babies found with valid birth and discharge dates.")

        # KMC Initiation Analysis
        st.subheader("KMC Initiation Timing - Inborn vs Outborn")
        kmc_initiation = calculate_kmc_initiation_metrics(filtered_data)
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            babies_without_kmc = len(filtered_data) - kmc_initiation['total_babies_with_kmc']
            st.metric("Babies with KMC", f"{kmc_initiation['total_babies_with_kmc']} out of {len(filtered_data)}")
            st.caption(f"{babies_without_kmc} babies without KMC")
        with col2:
            st.metric("Avg Time to Initiation", f"{kmc_initiation['avg_time_to_initiation_hours']:.1f}h")
        with col3:
            st.metric("Within 24h", f"{kmc_initiation['within_24h_percentage']:.1f}%",
                     f"{kmc_initiation['within_24h_count']} babies")
        with col4:
            st.metric("Within 48h", f"{kmc_initiation['within_48h_percentage']:.1f}%",
                     f"{kmc_initiation['within_48h_count']} babies")
        
        # KMC initiation chart
        if kmc_initiation['total_babies_with_kmc'] > 0:
            fig = go.Figure(data=[go.Pie(
                labels=['Within 24h', '24-48h', '>48h'],
                values=[
                    kmc_initiation['within_24h_count'],
                    kmc_initiation['within_48h_count'] - kmc_initiation['within_24h_count'],
                    kmc_initiation['total_babies_with_kmc'] - kmc_initiation['within_48h_count']
                ],
                marker_colors=['#10B981', ANSH_COLORS['secondary'], '#EF4444']
            )])
            fig.update_layout(title="KMC Initiation Timing Distribution")
            st.plotly_chart(fig, width='stretch')

        # Detailed breakdown by Inborn/Outborn
        col1, col2 = st.columns(2)

        # Inborn statistics
        if kmc_initiation['inborn_stats']:
            with col1:
                st.subheader("Inborn Babies")
                inborn = kmc_initiation['inborn_stats']
                st.metric("Total Inborn with KMC", inborn['count'])
                st.metric("Avg Time to Initiation", f"{inborn['avg_time_hours']:.1f}h")
                st.metric("Within 24h", f"{inborn['within_24h_percentage']:.1f}%", f"{inborn['within_24h_count']} babies")
                st.metric("Within 48h", f"{inborn['within_48h_percentage']:.1f}%", f"{inborn['within_48h_count']} babies")

        # Outborn statistics
        if kmc_initiation['outborn_stats']:
            with col2:
                st.subheader("Outborn Babies")
                outborn = kmc_initiation['outborn_stats']
                st.metric("Total Outborn with KMC", outborn['count'])
                st.metric("Avg Time to Initiation", f"{outborn['avg_time_hours']:.1f}h")
                st.metric("Within 24h", f"{outborn['within_24h_percentage']:.1f}%", f"{outborn['within_24h_count']} babies")
                st.metric("Within 48h", f"{outborn['within_48h_percentage']:.1f}%", f"{outborn['within_48h_count']} babies")

        # Inborn by location breakdown
        if kmc_initiation['inborn_location_stats']:
            st.subheader("Inborn Babies by Current Location")
            location_data = []
            for location, stats in kmc_initiation['inborn_location_stats'].items():
                location_data.append({
                    'Location': location,
                    'Count': stats['count'],
                    'Avg Time (hours)': f"{stats['avg_time_hours']:.1f}",
                    'Within 24h': f"{stats['within_24h_percentage']:.1f}% ({stats['within_24h_count']})",
                    'Within 48h': f"{stats['within_48h_percentage']:.1f}% ({stats['within_48h_count']})"
                })

            if location_data:
                location_df = pd.DataFrame(location_data)
                st.dataframe(location_df, width='stretch', hide_index=True)

        # Average KMC Hours by Location
        st.subheader("Average KMC Hours by Location & Hospital")
        avg_kmc_data = calculate_average_kmc_by_location(filtered_data, start_date, end_date)
        
        if avg_kmc_data:
            avg_kmc_df_data = []
            for data in avg_kmc_data:
                avg_kmc_df_data.append({
                    'Hospital': data['hospital'],
                    'Location': data['location'],
                    'Avg Hours/Day': f"{data['avg_hours_per_day']:.1f}h",
                    'Avg Hours/Baby': f"{data['avg_hours_per_baby']:.1f}h",
                    'Baby Count': data['baby_count'],
                    'Observation Days': data['observation_days']
                })
            
            avg_kmc_df = pd.DataFrame(avg_kmc_df_data)
            st.dataframe(avg_kmc_df, width='stretch', hide_index=True)
        else:
            st.info("No KMC data found for the selected time period.")
        
        # Follow-up Analysis
        st.subheader("Follow-up Completion Analysis")
        followup_metrics = calculate_followup_metrics(followup_data, filtered_data)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Eligible", followup_metrics['total_eligible'])
        with col2:
            st.metric("Completed", followup_metrics['total_completed'])
        with col3:
            st.metric("Completion Rate", f"{followup_metrics['overall_completion_rate']:.1f}%")
        
        # Follow-up requirements info
        st.info("""
        **Follow-up Requirements:** (Excludes dead babies, checks baby and babybackup collections only)
        • Follow up 2: Due by discharge date + 3 days (checks followUpNumber = 2 in followUp array)
        • Follow up 7: Due by discharge date + 7 days (checks followUpNumber = 7 in followUp array)
        • Follow up 14: Due by discharge date + 14 days (checks followUpNumber = 14 in followUp array)
        • Follow up 28: Due by birth date + 29 days (checks followUpNumber = 28 in followUp array)
        
        **Note:** Only counts follow-ups due until yesterday - future due dates are not included in overdue counts.
        """)
        
        # Follow-up details table
        if followup_metrics['hospital_summary']:
            followup_df_data = []
            for item in followup_metrics['hospital_summary']:
                followup_df_data.append({
                    'Hospital': item['hospital'],
                    'Follow-up Type': item['followup_type'],
                    'Eligible': item['eligible'],
                    'Completed': item['completed'],
                    'Completion Rate': f"{item['completion_rate']:.1f}%",
                    'Due': item['due'],
                    'Overdue': item['overdue']
                })
            
            followup_df = pd.DataFrame(followup_df_data)
            st.dataframe(followup_df, width='stretch', hide_index=True)
            
            # Follow-up completion chart
            if len(followup_df_data) > 0:
                fig = px.bar(
                    followup_df,
                    x='Follow-up Type',
                    y='Completion Rate',
                    color='Hospital',
                    title="Follow-up Completion Rates by Type and Hospital",
                    text='Completion Rate'
                )
                fig.update_traces(texttemplate='%{text}', textposition='outside')
                fig.update_layout(yaxis_title="Completion Rate (%)")
                st.plotly_chart(fig, width='stretch')
        else:
            st.info("No follow-up data available for the selected criteria.")
        
        # Skin Contact Analysis (All Follow-ups except 28)
        st.subheader("Skin Contact Analysis (All Follow-ups except Follow-up 28)")
        skin_contact_metrics = calculate_skin_contact_metrics(baby_data)

        # Display alerts for high skin contact values (> 10)
        if skin_contact_metrics.get('high_skin_contact_alerts', []):
            st.error("⚠️ **Alert: Babies with Skin Contact > 10**")
            alert_df = pd.DataFrame(skin_contact_metrics['high_skin_contact_alerts'])
            alert_df = alert_df.sort_values('numberSkinContact', ascending=False)
            st.dataframe(
                alert_df[['UID', 'followUpNumber', 'numberSkinContact', 'hospital']],
                width='stretch',
                hide_index=True
            )
            st.write(f"**Total alerts: {len(skin_contact_metrics['high_skin_contact_alerts'])} records**")

        if skin_contact_metrics['total_babies_with_data'] > 0:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Babies with Data", skin_contact_metrics['total_babies_with_data'])
            with col2:
                st.metric("Average Skin Contact", f"{skin_contact_metrics['average_skin_contact']:.1f}")
            with col3:
                st.metric("Min Value", f"{skin_contact_metrics['min_skin_contact']:.1f}")
            with col4:
                st.metric("Max Value", f"{skin_contact_metrics['max_skin_contact']:.1f}")

            # Show distribution chart
            if skin_contact_metrics['skin_contact_data']:
                df_skin = pd.DataFrame(skin_contact_metrics['skin_contact_data'])
                fig = px.histogram(
                    df_skin,
                    x='numberSkinContact',
                    title="Distribution of Skin Contact Values (Excluding Follow-up 28)",
                    nbins=20,
                    color_discrete_sequence=[ANSH_COLORS['primary']]
                )
                fig.update_layout(
                    xaxis_title="Number of Skin Contact",
                    yaxis_title="Count of Babies"
                )
                st.plotly_chart(fig, width='stretch')
                
                # Show data by hospital
                hospital_skin_summary = df_skin.groupby('hospital').agg({
                    'numberSkinContact': ['count', 'mean', 'min', 'max']
                }).round(1)
                hospital_skin_summary.columns = ['Count', 'Average', 'Min', 'Max']
                st.subheader("Skin Contact by Hospital")
                st.dataframe(hospital_skin_summary, width='stretch')
        else:
            st.info("No skin contact data found in follow-up 28 records.")

        # Discharge Outcomes Analysis
        st.subheader("Discharge Outcomes Analysis")
        st.caption("Based on discharges and babyBackUp collections with updated categorization rules")

        discharge_outcomes = calculate_discharge_outcomes(filtered_data, discharge_data)

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Discharged", discharge_outcomes['total_discharged'])
            st.caption(f"({discharge_outcomes['unique_babies_processed']} unique babies)")
        with col2:
            st.metric("Critical & Home", f"{discharge_outcomes['critical_home_percentage']:.1f}%",
                     f"{discharge_outcomes['categories']['critical_home']['count']} babies")
        with col3:
            st.metric("Stable & Home", f"{discharge_outcomes['stable_home_percentage']:.1f}%",
                     f"{discharge_outcomes['categories']['stable_home']['count']} babies")
        with col4:
            st.metric("Deaths", f"{discharge_outcomes['died_percentage']:.1f}%",
                     f"{discharge_outcomes['categories']['died']['count']} babies")

        # Discharge outcomes pie chart
        if discharge_outcomes['total_discharged'] > 0:
            fig = go.Figure(data=[go.Pie(
                labels=['Critical & Home', 'Stable & Home', 'Critical & Referred', 'Died', 'Other'],
                values=[
                    discharge_outcomes['categories']['critical_home']['count'],
                    discharge_outcomes['categories']['stable_home']['count'],
                    discharge_outcomes['categories']['critical_referred']['count'],
                    discharge_outcomes['categories']['died']['count'],
                    discharge_outcomes['categories']['other']['count']
                ],
                marker_colors=[ANSH_COLORS['secondary'], '#10B981', '#F59E0B', '#EF4444', '#9CA3AF']
            )])
            fig.update_layout(title="Discharge Outcomes Distribution (Discharges + BabyBackUp Collections)")
            st.plotly_chart(fig, width='stretch')

        # Show detailed breakdown table
        st.subheader("Detailed Discharge Breakdown")
        detailed_discharge_data = []
        category_names = {
            'critical_home': 'Critical and sent home',
            'stable_home': 'Stable and sent home',
            'critical_referred': 'Critical and referred',
            'died': 'Died',
            'other': 'Other/Unknown'
        }

        for category, data in discharge_outcomes['categories'].items():
            percentage = (data['count'] / discharge_outcomes['total_discharged'] * 100) if discharge_outcomes['total_discharged'] > 0 else 0
            detailed_discharge_data.append({
                'Discharge Category': category_names.get(category, category),
                'Count': data['count'],
                'Percentage': f"{percentage:.1f}%"
            })

        discharge_breakdown_df = pd.DataFrame(detailed_discharge_data)
        st.dataframe(discharge_breakdown_df, width='stretch', hide_index=True)

        # Show collection sources breakdown
        with st.expander("📊 View Data Sources Breakdown"):
            st.write("**Data Sources Used:**")

            discharge_sources = {}
            for category, data in discharge_outcomes['categories'].items():
                for baby_info in data['babies']:
                    source = baby_info['source']
                    if source not in discharge_sources:
                        discharge_sources[source] = 0
                    discharge_sources[source] += 1

            col1, col2 = st.columns(2)
            with col1:
                st.metric("Discharges Collection", discharge_sources.get('discharges collection', 0))
            with col2:
                st.metric("BabyBackUp Collection", discharge_sources.get('babyBackUp collection', 0))

            st.write("**Categorization Rules Applied:**")
            st.markdown("""
            **From Discharges Collection:**
            - Critical and sent home: `dischargeStatus = critical AND dischargeType = home`
            - Stable and sent home: `dischargeStatus = stable AND dischargeType = home`
            - Critical and referred: `dischargeStatus = critical AND dischargeType = referred`
            - Died: `dischargeType = died`

            **From BabyBackUp Collection:**
            - Critical and sent home: `dischargedStatusString = "Critical and discharged"`
            - Stable and sent home: `dischargedStatusString = "Discharged according to criteria/stable"`
            - Critical and referred: `dischargedStatusString = "Referred out/Critical"`
            - Died: `dischargedStatusString = "डिस्चार्ज से पहले ही मृत्यु हो गई 👼" OR "died before discharge"`
            """)

        # Critical Reasons Analysis
        st.subheader("Critical Reasons Analysis")
        st.caption("Individual critical reasons from discharge collection (only babies with critical reasons data)")

        critical_reasons_data = calculate_individual_critical_reasons(discharge_data)

        if critical_reasons_data['total_babies_with_reasons'] > 0:
            # Overview metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Babies with Critical Reasons", critical_reasons_data['total_babies_with_reasons'])
            with col2:
                st.metric("Unique Critical Reasons", critical_reasons_data['total_unique_reasons'])
            with col3:
                total_discharges = len(discharge_data)
                percentage_with_reasons = (critical_reasons_data['total_babies_with_reasons'] / total_discharges * 100) if total_discharges > 0 else 0
                st.metric("Percentage with Reasons", f"{percentage_with_reasons:.1f}%")

            # Create chart data - show top 10 most common reasons
            reasons_list = []
            for reason, data in critical_reasons_data['individual_reasons'].items():
                reasons_list.append({
                    'Reason': reason,
                    'Count': data['count'],
                    'Percentage': (data['count'] / critical_reasons_data['total_babies_with_reasons'] * 100)
                })

            # Sort by count and take top 10
            reasons_list_sorted = sorted(reasons_list, key=lambda x: x['Count'], reverse=True)[:10]

            if reasons_list_sorted:
                # Create horizontal bar chart
                df_reasons = pd.DataFrame(reasons_list_sorted)

                fig = px.bar(
                    df_reasons,
                    y='Reason',
                    x='Count',
                    orientation='h',
                    title="Top 10 Critical Reasons (Individual Count)",
                    color='Count',
                    color_continuous_scale='Blues'
                )
                fig.update_layout(
                    height=500,
                    xaxis_title="Number of Babies",
                    yaxis_title="Critical Reason",
                    yaxis={'categoryorder': 'total ascending'}
                )
                st.plotly_chart(fig, width='stretch')

                # Show detailed table
                st.subheader("Detailed Critical Reasons Breakdown")

                # Create comprehensive table with all reasons
                all_reasons_data = []
                for reason, data in sorted(critical_reasons_data['individual_reasons'].items(), key=lambda x: x[1]['count'], reverse=True):
                    percentage = (data['count'] / critical_reasons_data['total_babies_with_reasons'] * 100)
                    all_reasons_data.append({
                        'Critical Reason': reason,
                        'Count': data['count'],
                        'Percentage': f"{percentage:.1f}%"
                    })

                reasons_df = pd.DataFrame(all_reasons_data)
                st.dataframe(reasons_df, width='stretch', hide_index=True)

                # Show explanation
                with st.expander("📋 Critical Reasons Explanation"):
                    st.markdown("""
                    **Common Critical Reasons:**
                    - **weightLoss>2%**: Baby lost more than 2% of birth weight
                    - **GA**: Gestational Age related concerns
                    - **dangerSigns**: Baby showing danger signs
                    - **notSingleBaby**: Multiple birth (twins, triplets, etc.)
                    - **dischargeWeight**: Weight-related concerns at discharge
                    - **inHospital<1Day**: Baby hospitalized for less than 1 day
                    - **dischargeTemperature**: Temperature concerns at discharge
                    - **dischargeRRawake**: Respiratory rate issues when awake
                    - **badFeeding**: Poor feeding patterns

                    **Note**: Some babies may have multiple critical reasons, so they are counted for each applicable reason.
                    """)
        else:
            st.info("No critical reasons data found in the discharge collection.")


    with tab3:
        st.header("Mortality Analysis")
        
        # Death rate metrics
        death_metrics = calculate_death_rates(filtered_data, discharge_data)
        
        # Overview metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Babies", death_metrics['total_babies'])
        with col2:
            st.metric("Deaths", death_metrics['dead_babies'])
        with col3:
            st.metric("Mortality Rate", f"{death_metrics['mortality_rate']:.2f}%")
        with col4:
            # Calculate survival rate
            survival_rate = 100 - death_metrics['mortality_rate']
            st.metric("Survival Rate", f"{survival_rate:.2f}%")
        
        # Create tabs for different analyses
        mort_tab1, mort_tab2, mort_tab3, mort_tab4, mort_tab5 = st.tabs([
            "🏥 By Hospital", "📊 Demographics", "📍 By Location", "💊 KMC Stability", "📋 Detailed Data"
        ])
        
        with mort_tab1:
            st.subheader("Mortality Rate by Hospital")
            if death_metrics['hospital_data']['hospitals']:
                fig = go.Figure()
                
                # Add bar chart for counts
                fig.add_trace(go.Bar(
                    name='Total Babies',
                    x=death_metrics['hospital_data']['hospitals'],
                    y=death_metrics['hospital_data']['totals'],
                    marker_color=ANSH_COLORS['light'],
                    yaxis='y'
                ))
                
                fig.add_trace(go.Bar(
                    name='Deaths',
                    x=death_metrics['hospital_data']['hospitals'],
                    y=death_metrics['hospital_data']['deaths'],
                    marker_color='#EF4444',
                    yaxis='y'
                ))
                
                # Add line for mortality rate
                fig.add_trace(go.Scatter(
                    name='Mortality Rate (%)',
                    x=death_metrics['hospital_data']['hospitals'],
                    y=death_metrics['hospital_data']['rates'],
                    mode='lines+markers',
                    marker_color=ANSH_COLORS['primary'],
                    line=dict(width=3),
                    yaxis='y2'
                ))
                
                fig.update_layout(
                    title="Hospital-wise Mortality Analysis",
                    xaxis_title="Hospital",
                    yaxis=dict(title="Number of Babies", side="left"),
                    yaxis2=dict(title="Mortality Rate (%)", side="right", overlaying="y"),
                    barmode='group'
                )
                st.plotly_chart(fig, width='stretch')
        
        with mort_tab2:
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Inborn vs Outborn")
                birth_place = death_metrics['birth_place']
                
                if birth_place['inborn']['total'] > 0 or birth_place['outborn']['total'] > 0:
                    inborn_rate = (birth_place['inborn']['deaths'] / birth_place['inborn']['total'] * 100) if birth_place['inborn']['total'] > 0 else 0
                    outborn_rate = (birth_place['outborn']['deaths'] / birth_place['outborn']['total'] * 100) if birth_place['outborn']['total'] > 0 else 0
                    
                    fig = go.Figure(data=[
                        go.Bar(name='Total', x=['Inborn', 'Outborn'], 
                              y=[birth_place['inborn']['total'], birth_place['outborn']['total']],
                              marker_color=ANSH_COLORS['light']),
                        go.Bar(name='Deaths', x=['Inborn', 'Outborn'], 
                              y=[birth_place['inborn']['deaths'], birth_place['outborn']['deaths']],
                              marker_color='#EF4444')
                    ])
                    
                    fig.update_layout(
                        title="Mortality: Inborn vs Outborn",
                        barmode='group',
                        yaxis_title="Number of Babies"
                    )
                    st.plotly_chart(fig, width='stretch')
                    
                    # Show rates
                    st.metric("Inborn Mortality Rate", f"{inborn_rate:.2f}%", 
                             f"{birth_place['inborn']['deaths']}/{birth_place['inborn']['total']}")
                    st.metric("Outborn Mortality Rate", f"{outborn_rate:.2f}%",
                             f"{birth_place['outborn']['deaths']}/{birth_place['outborn']['total']}")
            
            with col2:
                # Discharge Categorization Analysis (ONLY for dead babies)
                st.subheader("Dead Babies by Discharge Category")
                st.caption("Analysis based on deadBaby = true field only")
                
                # Get the detailed discharge outcomes
                discharge_outcomes = death_metrics['discharge_outcomes']
                
                if discharge_outcomes['total_discharged'] > 0:
                    category_names = {
                        'critical_home': 'Critical and sent home',
                        'stable_home': 'Stable and sent home', 
                        'critical_referred': 'Critical and referred',
                        'died': 'Died',
                        'other': 'Other/Unknown'
                    }
                    
                    discharge_category_data = []
                    for category, data in discharge_outcomes['categories'].items():
                        if data['count'] > 0:  # Only show categories with dead babies
                            discharge_category_data.append({
                                'Discharge Category': category_names.get(category, category),
                                'Dead Babies': data['count'],
                                'Percentage of Dead Babies': f"{(data['count'] / discharge_outcomes['total_discharged'] * 100):.1f}%" if discharge_outcomes['total_discharged'] > 0 else "0%"
                            })
                    
                    discharge_cat_df = pd.DataFrame(discharge_category_data)
                    st.dataframe(discharge_cat_df, width='stretch', hide_index=True)
                    
                    # Show pie chart of discharge categories (only for dead babies)
                    categories_with_deaths = [(cat, data) for cat, data in discharge_outcomes['categories'].items() if data['count'] > 0]
                    if categories_with_deaths:
                        fig = go.Figure(data=[go.Pie(
                            labels=[category_names.get(cat, cat) for cat, data in categories_with_deaths],
                            values=[data['count'] for cat, data in categories_with_deaths],
                            marker_colors=[ANSH_COLORS['secondary'], '#10B981', '#F59E0B', '#EF4444', '#9CA3AF']
                        )])
                        fig.update_layout(title="Dead Babies Distribution by Discharge Category")
                        st.plotly_chart(fig, width='stretch')
                    else:
                        st.info("No dead babies found in the current dataset.")
            
            # Show detailed dead baby breakdown by discharge category
            with st.expander("🔍 View detailed dead babies by discharge category"):
                outcomes = death_metrics['discharge_outcomes']
                
                if outcomes['total_discharged'] > 0:
                    category_names = {
                        'critical_home': 'Critical and sent home',
                        'stable_home': 'Stable and sent home', 
                        'critical_referred': 'Critical and referred',
                        'died': 'Died before discharge',
                        'other': 'Other/Unknown discharge status'
                    }
                    
                    for category, data in outcomes['categories'].items():
                        if data['count'] > 0:
                            st.write(f"**{category_names.get(category, category)}:** {data['count']} dead babies")
                            
                            # Show sample UIDs with discharge info
                            sample_babies = data['babies'][:5]  # Show first 5
                            for baby_info in sample_babies:
                                source_info = f"Source: {baby_info.get('source', 'Unknown')}"
                                discharge_detail = baby_info.get('dischargeStatusString', 'No status string')
                                
                                st.write(f"- **{baby_info['UID']}** ({baby_info['hospitalName']})")
                                st.write(f"  - {source_info}")
                                st.write(f"  - Discharge Status: {discharge_detail}")
                                st.write("")
                                
                            if len(data['babies']) > 5:
                                st.write(f"... and {len(data['babies']) - 5} more dead babies")
                            st.write("---")
                else:
                    st.info("No dead babies found in current dataset.")
        
        with mort_tab3:
            st.subheader("Mortality by Current Location")
            location_data = death_metrics['location_analysis']
            
            if location_data:
                location_df_data = []
                for location, data in location_data.items():
                    rate = (data['deaths'] / data['total'] * 100) if data['total'] > 0 else 0
                    location_df_data.append({
                        'Location': location,
                        'Total Babies': data['total'],
                        'Deaths': data['deaths'],
                        'Mortality Rate (%)': f"{rate:.2f}%"
                    })
                
                location_df = pd.DataFrame(location_df_data)
                st.dataframe(location_df, width='stretch', hide_index=True)
                
                # Visualization
                locations = [item['Location'] for item in location_df_data]
                rates = [float(item['Mortality Rate (%)'].replace('%', '')) for item in location_df_data]
                
                fig = px.bar(
                    x=locations,
                    y=rates,
                    title="Mortality Rate by Location",
                    color=rates,
                    color_continuous_scale=['#10B981', '#F59E0B', '#EF4444']
                )
                fig.update_layout(
                    xaxis_title="Location",
                    yaxis_title="Mortality Rate (%)",
                    showlegend=False
                )
                st.plotly_chart(fig, width='stretch')
        
        with mort_tab4:
            st.subheader("KMC Stability Analysis")
            st.caption("Unstable = 0 KMC hours AND (unstableForKMC=true OR danger sign 'केएमसी के लिए अस्थिर 🦘🚫')")
            
            kmc_data = death_metrics['kmc_stability']
            
            col1, col2 = st.columns(2)
            
            with col1:
                stable_rate = (kmc_data['stable']['deaths'] / kmc_data['stable']['total'] * 100) if kmc_data['stable']['total'] > 0 else 0
                unstable_rate = (kmc_data['unstable']['deaths'] / kmc_data['unstable']['total'] * 100) if kmc_data['unstable']['total'] > 0 else 0
                
                st.metric("KMC Stable Babies", kmc_data['stable']['total'])
                st.metric("Stable Mortality Rate", f"{stable_rate:.2f}%", f"{kmc_data['stable']['deaths']} deaths")
                
                st.metric("KMC Unstable Babies", kmc_data['unstable']['total'])
                st.metric("Unstable Mortality Rate", f"{unstable_rate:.2f}%", f"{kmc_data['unstable']['deaths']} deaths")
            
            with col2:
                if kmc_data['stable']['total'] > 0 or kmc_data['unstable']['total'] > 0:
                    fig = go.Figure(data=[
                        go.Bar(name='Total', x=['KMC Stable', 'KMC Unstable'], 
                              y=[kmc_data['stable']['total'], kmc_data['unstable']['total']],
                              marker_color=ANSH_COLORS['light']),
                        go.Bar(name='Deaths', x=['KMC Stable', 'KMC Unstable'], 
                              y=[kmc_data['stable']['deaths'], kmc_data['unstable']['deaths']],
                              marker_color='#EF4444')
                    ])
                    
                    fig.update_layout(
                        title="Mortality: KMC Stability",
                        barmode='group',
                        yaxis_title="Number of Babies"
                    )
                    st.plotly_chart(fig, width='stretch')
        
        with mort_tab5:
            st.subheader("Detailed Mortality Data")
            st.caption("All babies with deadBaby = true")
            
            # Get all dead babies from the data, not just from discharge outcomes
            all_dead_babies = []
            processed_uids = set()
            
            for baby in baby_data:
                uid = baby.get('UID')
                if baby.get('deadBaby') == True and uid and uid not in processed_uids:
                    processed_uids.add(uid)
                    all_dead_babies.append(baby)
            
            if all_dead_babies:
                st.write(f"**Showing {len(all_dead_babies)} deceased babies (deadBaby = true):**")
                
                detailed_data = []
                for baby in all_dead_babies:
                    birth_date = convert_unix_to_datetime(baby.get('dateOfBirth'))
                    
                    # Check KMC stability using updated criteria
                    kmc_status = check_kmc_stability(baby)
                    
                    # Calculate total KMC time
                    total_kmc_time = 0
                    for obs_day in baby.get('observationDay', []):
                        total_kmc_time += obs_day.get('totalKMCtimeDay', 0)
                    
                    # Get PC Note - look for PCsNote in baby and babybackup collections
                    pc_note = baby.get('PCsNote', baby.get('pcNote', 'No note'))
                    if len(str(pc_note)) > 100:
                        pc_note = str(pc_note)[:100] + '...'
                    
                    # Determine discharge category for this dead baby
                    category = 'other'
                    # Check if from discharge collection
                    matching_discharge = None
                    for discharge in discharge_data:
                        if discharge.get('UID') == baby.get('UID'):
                            matching_discharge = discharge
                            break
                    
                    if matching_discharge:
                        category_result = categorize_discharge_from_collection(matching_discharge, 'discharges')
                        # Map to display names
                        category_map = {
                            'critical_home': 'Critical & Home',
                            'stable_home': 'Stable & Home',
                            'critical_referred': 'Critical & Referred',
                            'died': 'Died',
                            'other': 'Other'
                        }
                        category = category_map.get(category_result, 'Other')
                    elif baby.get('source') == 'babyBackUp':
                        category_result = categorize_discharge_from_collection(baby, 'babyBackUp')
                        # Map to display names
                        category_map = {
                            'critical_home': 'Critical & Home',
                            'stable_home': 'Stable & Home',
                            'critical_referred': 'Critical & Referred',
                            'died': 'Died',
                            'other': 'Other'
                        }
                        category = category_map.get(category_result, 'Other')
                    
                    detailed_data.append({
                        'UID': baby.get('UID', 'N/A'),
                        'Hospital': baby.get('hospitalName', 'Unknown'),
                        'Source': baby.get('source', 'Unknown'),
                        'Birth Date': birth_date.strftime('%Y-%m-%d') if birth_date else 'Invalid',
                        'Birth Weight (g)': baby.get('birthWeight', 'N/A'),
                        'Current Location': baby.get('currentLocationOfTheBaby', 'Unknown'),
                        'Place of Delivery': 'Inborn' if baby.get('placeOfDelivery') in ['यह अस्पताल', 'this hospital'] else 'Outborn',
                        'KMC Status': 'Unstable for KMC' if kmc_status == 'unstable' else 'Stable',
                        'Total KMC Hours': f"{total_kmc_time / 60:.1f}h" if total_kmc_time > 0 else "0h",
                        'Discharge Category': category,
                        'Discharge Status String': baby.get('dischargeStatusString', 'N/A'),
                        'PC Note': pc_note
                    })
                
                detailed_df = pd.DataFrame(detailed_data)
                st.dataframe(detailed_df, width='stretch')
                
                # Show summary by category
                category_summary = detailed_df['Discharge Category'].value_counts()
                st.subheader("Dead Babies by Discharge Category Summary")
                st.bar_chart(category_summary)
                
                # Download option
                csv = detailed_df.to_csv(index=False)
                st.download_button(
                    label="Download Deceased Babies Data (CSV)",
                    data=csv,
                    file_name=f"deceased_babies_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
            else:
                st.info("No deceased babies found in the current filtered data.")
    
    with tab4:
        st.header("Daily KMC Analysis - Last 3 Days")
        st.caption("Average KMC hours by hospital and baby location (Current Location of the Baby)")
        
        analysis_data, hospitals, locations, excluded_counts = calculate_daily_kmc_analysis(filtered_data)
        
        for date_key in sorted(analysis_data.keys(), reverse=True):
            date_obj = datetime.strptime(date_key, '%Y-%m-%d')
            excluded_count = excluded_counts.get(date_key, 0)

            if excluded_count > 0:
                st.subheader(f"{date_obj.strftime('%A, %B %d, %Y')}")
                st.info(f"ℹ️ **{excluded_count} babies excluded** from analysis as they were discharged on this date")
            else:
                st.subheader(f"{date_obj.strftime('%A, %B %d, %Y')}")
                st.info("ℹ️ **0 babies excluded** - no same-day discharges")
            
            # Create styled table data
            table_data = []
            for location in locations:
                row = {'Location': location}
                for hospital in hospitals:
                    data = analysis_data[date_key].get(hospital, {}).get(location, {})
                    avg_hours = data.get('average_kmc_hours', 0)
                    baby_count = data.get('baby_count', 0)
                    
                    if baby_count > 0:
                        # Color coding based on hours
                        if avg_hours >= 6:
                            color = "#10B981"  # Green
                            emoji = "🟢"
                        elif avg_hours >= 4:
                            color = "#F59E0B"  # Yellow
                            emoji = "🟡"
                        elif avg_hours >= 1:
                            color = "#F97316"  # Orange
                            emoji = "🟠"
                        else:
                            color = "#EF4444"  # Red
                            emoji = "🔴"
                        
                        row[hospital] = f"{emoji} {avg_hours:.1f}h ({baby_count})"
                    else:
                        row[hospital] = "-"
                table_data.append(row)
            
            # Display colored table
            df = pd.DataFrame(table_data)
            st.dataframe(df, width='stretch')
            
            # Add expandable details for each hospital-location combination
            with st.expander(f"📋 View detailed data for {date_obj.strftime('%B %d, %Y')}"):
                for hospital in hospitals:
                    if any(analysis_data[date_key].get(hospital, {}).get(loc, {}).get('baby_count', 0) > 0 
                           for loc in locations):
                        st.write(f"**{hospital}**")
                        
                        for location in locations:
                            data = analysis_data[date_key].get(hospital, {}).get(location, {})
                            if data.get('baby_count', 0) > 0:
                                with st.container():
                                    col1, col2, col3 = st.columns([2, 1, 1])
                                    with col1:
                                        st.write(f"📍 {location}")
                                    with col2:
                                        st.metric("Babies", data['baby_count'])
                                    with col3:
                                        st.metric("Avg Hours", f"{data['average_kmc_hours']:.1f}h")
                                    
                                    # Show individual baby data if possible
                                    location_babies = []
                                    target_date = datetime.strptime(date_key, '%Y-%m-%d').date()
                                    
                                    for baby in filtered_data:
                                        if (baby.get('hospitalName') == hospital and 
                                            baby.get('currentLocationOfTheBaby') == location):
                                            
                                            birth_date = convert_unix_to_datetime(baby.get('dateOfBirth'))
                                            if birth_date:
                                                for obs_day in baby.get('observationDay', []):
                                                    if obs_day.get('ageDay') is not None:
                                                        obs_date = birth_date.date() + timedelta(days=obs_day.get('ageDay', 0))
                                                        if (obs_date == target_date and 
                                                            obs_day.get('totalKMCtimeDay', 0) > 0):
                                                            location_babies.append({
                                                                'UID': baby.get('UID', 'N/A'),
                                                                'KMC Hours': f"{obs_day['totalKMCtimeDay'] / 60:.1f}h",
                                                                'KMC Minutes': obs_day['totalKMCtimeDay']
                                                            })
                                    
                                    if location_babies:
                                        baby_df = pd.DataFrame(location_babies)
                                        st.dataframe(baby_df, width='stretch', hide_index=True)
                                
                                st.divider()
            
            st.markdown("**Legend:** 🟢 ≥6h (Excellent) | 🟡 4-6h (Good) | 🟠 1-4h (Needs Improvement) | 🔴 <1h (Critical)")
            st.markdown("---")
    
    with tab5:
        st.header("Data Quality Monitoring & Classification")
        st.caption("Analysis of data completeness, accuracy, and baby classification")

        # Create sub-tabs for different monitoring aspects
        mon_tab1, mon_tab2 = st.tabs(["📝 KMC Verification", "📊 Observations Verification"])

        with mon_tab1:
            st.subheader("KMC Verification Monitoring")
            st.info("New verification system: correct, incorrect, unable to verify, not verified")

            # Debug information
            with st.expander("🔍 Debug: KMC Verification Data"):
                st.write(f"Total babies in filtered_data: {len(filtered_data)}")
                sample_baby = filtered_data[0] if filtered_data else {}
                st.write(f"Sample baby observation structure:")
                obs_days = sample_baby.get('observationDay', [])
                st.write(f"- observationDay count: {len(obs_days)}")
                if obs_days:
                    first_obs = obs_days[0]
                    st.write(f"- Sample observation keys: {list(first_obs.keys())}")
                    st.write(f"- filledCorrectly: {first_obs.get('filledCorrectly', 'N/A')}")
                    st.write(f"- kmcfilledcorrectly: {first_obs.get('kmcfilledcorrectly', 'N/A')}")
                    st.write(f"- mnecomment: {first_obs.get('mnecomment', 'N/A')}")

            kmc_verification = calculate_kmc_verification_monitoring(filtered_data)

            if kmc_verification['verification_stats']['total_observations'] > 0:
                # Summary metrics
                stats = kmc_verification['verification_stats']
                col1, col2, col3, col4, col5 = st.columns(5)

                with col1:
                    st.metric("Total Observations", stats['total_observations'])
                with col2:
                    correct_pct = (stats['correct'] / stats['total_observations'] * 100) if stats['total_observations'] > 0 else 0
                    st.metric("Correct", f"{stats['correct']} ({correct_pct:.1f}%)")
                with col3:
                    incorrect_pct = (stats['incorrect'] / stats['total_observations'] * 100) if stats['total_observations'] > 0 else 0
                    st.metric("Incorrect", f"{stats['incorrect']} ({incorrect_pct:.1f}%)")
                with col4:
                    unable_pct = (stats['unable_to_verify'] / stats['total_observations'] * 100) if stats['total_observations'] > 0 else 0
                    st.metric("Unable to Verify", f"{stats['unable_to_verify']} ({unable_pct:.1f}%)")
                with col5:
                    not_verified_pct = (stats['not_verified'] / stats['total_observations'] * 100) if stats['total_observations'] > 0 else 0
                    st.metric("Not Verified", f"{stats['not_verified']} ({not_verified_pct:.1f}%)")

                # Pie chart
                fig = go.Figure(data=[go.Pie(
                    labels=['Correct', 'Incorrect', 'Unable to Verify', 'Not Verified'],
                    values=[stats['correct'], stats['incorrect'], stats['unable_to_verify'], stats['not_verified']],
                    marker_colors=['#10B981', '#EF4444', '#F59E0B', '#9CA3AF']
                )])
                fig.update_layout(title="KMC Verification Status Distribution")
                st.plotly_chart(fig, width='stretch')

                # Show problematic entries
                problematic = [entry for entry in kmc_verification['detailed_data'] if entry['status'] in ['incorrect', 'unable_to_verify']]
                if problematic:
                    st.subheader(f"Problematic KMC Entries ({len(problematic)})")
                    problem_df = pd.DataFrame(problematic)
                    st.dataframe(problem_df, width='stretch')

                # Show detailed table with observation data and mnecomment
                entries_with_comments = [entry for entry in kmc_verification['detailed_data'] if entry.get('mnecomment') and entry['mnecomment'].strip()]
                if entries_with_comments:
                    st.subheader(f"Detailed KMC Entries with Comments ({len(entries_with_comments)})")

                    # Create a flattened dataframe for display
                    detailed_rows = []
                    for entry in entries_with_comments:
                        base_row = {
                            'UID': entry['UID'],
                            'AgeDay': entry['ageDay'],
                            'Hospital': entry['hospitalName'],
                            'Date': entry['observationDate'],
                            'Status': entry['status'],
                            'MNE Comment': entry['mnecomment']
                        }

                        # Add key observation data fields
                        obs_data = entry.get('observation_data', {})
                        for key, value in obs_data.items():
                            if value is not None and str(value).strip():  # Only include non-empty values
                                base_row[f'obs_{key}'] = value

                        detailed_rows.append(base_row)

                    if detailed_rows:
                        detailed_df = pd.DataFrame(detailed_rows)
                        st.dataframe(detailed_df, width='stretch', hide_index=True)

            else:
                st.info("No KMC verification data found")

        with mon_tab2:
            st.subheader("Observations Verification Monitoring")
            st.info("Verification status: correct/not checked vs incorrect")

            obs_verification = calculate_observations_verification_monitoring(filtered_data)

            if obs_verification['verification_stats']['total_observations'] > 0:
                stats = obs_verification['verification_stats']
                col1, col2, col3 = st.columns(3)

                with col1:
                    st.metric("Total Observations", stats['total_observations'])
                with col2:
                    correct_pct = (stats['correct_or_not_checked'] / stats['total_observations'] * 100) if stats['total_observations'] > 0 else 0
                    st.metric("Correct/Not Checked", f"{stats['correct_or_not_checked']} ({correct_pct:.1f}%)")
                with col3:
                    incorrect_pct = (stats['incorrect'] / stats['total_observations'] * 100) if stats['total_observations'] > 0 else 0
                    st.metric("Incorrect", f"{stats['incorrect']} ({incorrect_pct:.1f}%)")

                # Pie chart
                fig = go.Figure(data=[go.Pie(
                    labels=['Correct/Not Checked', 'Incorrect'],
                    values=[stats['correct_or_not_checked'], stats['incorrect']],
                    marker_colors=['#10B981', '#EF4444']
                )])
                fig.update_layout(title="Observations Verification Status Distribution")
                st.plotly_chart(fig, width='stretch')

                # Show incorrect entries
                incorrect_entries = [entry for entry in obs_verification['detailed_data'] if entry['status'] == 'incorrect']
                if incorrect_entries:
                    st.subheader(f"Incorrect Observation Entries ({len(incorrect_entries)})")
                    incorrect_df = pd.DataFrame(incorrect_entries)
                    st.dataframe(incorrect_df, width='stretch')
                else:
                    st.success("✅ No incorrect observation entries found!")

                # Show detailed table with observation data and mnecomment
                entries_with_comments_obs = [entry for entry in obs_verification['detailed_data'] if entry.get('mnecomment') and entry['mnecomment'].strip()]
                if entries_with_comments_obs:
                    st.subheader(f"Detailed Observation Entries with Comments ({len(entries_with_comments_obs)})")

                    # Create a flattened dataframe for display
                    detailed_obs_rows = []
                    for entry in entries_with_comments_obs:
                        base_row = {
                            'UID': entry['UID'],
                            'AgeDay': entry['ageDay'],
                            'Hospital': entry['hospitalName'],
                            'Date': entry['observationDate'],
                            'Status': entry['status'],
                            'MNE Comment': entry['mnecomment']
                        }

                        # Add key observation data fields
                        obs_data = entry.get('observation_data', {})
                        for key, value in obs_data.items():
                            if value is not None and str(value).strip():  # Only include non-empty values
                                base_row[f'obs_{key}'] = value

                        detailed_obs_rows.append(base_row)

                    if detailed_obs_rows:
                        detailed_obs_df = pd.DataFrame(detailed_obs_rows)
                        st.dataframe(detailed_obs_df, width='stretch', hide_index=True)

            else:
                st.info("No observation verification data found")
    
    with tab6:
        st.header("Comprehensive Baby Data Explorer")
        st.caption("Individual baby metrics with KMC data, follow-ups, and clinical information")

        # Calculate comprehensive baby metrics
        if filtered_data:
            baby_metrics = calculate_individual_baby_metrics(filtered_data)

            if baby_metrics:
                # Additional filtering options
                col1, col2, col3 = st.columns(3)

                with col1:
                    death_filter = st.selectbox(
                        "Filter by Death Status",
                        ["All", "Dead", "Alive"],
                        key="death_filter"
                    )

                with col2:
                    location_options = ["All"] + sorted(list(set(m['Location'] for m in baby_metrics if m['Location'] != 'Unknown')))
                    location_filter = st.selectbox(
                        "Filter by Location",
                        location_options,
                        key="location_filter"
                    )

                with col3:
                    kmc_filter = st.selectbox(
                        "Filter by KMC Status",
                        ["All", "Has KMC Data", "No KMC Data"],
                        key="kmc_filter"
                    )

                # Apply additional filters
                filtered_metrics = baby_metrics.copy()

                if death_filter != "All":
                    filtered_metrics = [m for m in filtered_metrics if
                                     (m['Dead Baby'] == 'Yes' if death_filter == "Dead" else m['Dead Baby'] == 'No')]

                if location_filter != "All":
                    filtered_metrics = [m for m in filtered_metrics if m['Location'] == location_filter]

                if kmc_filter != "All":
                    if kmc_filter == "Has KMC Data":
                        filtered_metrics = [m for m in filtered_metrics if m['KMC Days Count'] > 0]
                    else:
                        filtered_metrics = [m for m in filtered_metrics if m['KMC Days Count'] == 0]

                # Limit to first 200 for performance
                display_metrics = filtered_metrics[:200]

                if display_metrics:
                    # Create DataFrame with selected columns
                    df_data = []
                    for metrics in display_metrics:
                        df_data.append({
                            'UID': metrics['UID'],
                            'Mother Name': metrics['Mother Name'],
                            'Hospital': metrics['Hospital'],
                            'Location': metrics['Location'],
                            'Total KMC Hours': metrics['Total KMC Hours'],
                            'Avg KMC Hours/Day': metrics['Avg KMC Hours/Day'],
                            'KMC Days': metrics['KMC Days Count'],
                            'Follow-up 2': metrics['Follow-up 2'],
                            'Follow-up 7': metrics['Follow-up 7'],
                            'Follow-up 14': metrics['Follow-up 14'],
                            'Follow-up 28': metrics['Follow-up 28'],
                            'Dead Baby': metrics['Dead Baby'],
                            'Danger Signs': metrics['Danger Signs'],
                            'Birth Date': metrics['Birth Date'].strftime('%Y-%m-%d') if metrics['Birth Date'] else 'Invalid',
                            'Source': metrics['Source']
                        })

                    df = pd.DataFrame(df_data)

                    # Display summary stats
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total Babies", len(display_metrics))
                    with col2:
                        babies_with_kmc = len([m for m in display_metrics if m['KMC Days Count'] > 0])
                        st.metric("Babies with KMC", babies_with_kmc)
                    with col3:
                        dead_babies = len([m for m in display_metrics if m['Dead Baby'] == 'Yes'])
                        st.metric("Dead Babies", dead_babies)
                    with col4:
                        if babies_with_kmc > 0:
                            avg_total_kmc = sum(float(m['Total KMC Hours'].replace('h', '')) for m in display_metrics if m['KMC Days Count'] > 0) / babies_with_kmc
                            st.metric("Avg Total KMC", f"{avg_total_kmc:.1f}h")

                    # Display the data table
                    st.dataframe(df, width='stretch', hide_index=True)

                    # Download CSV
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Baby Data CSV",
                        data=csv,
                        file_name=f"ansh_baby_metrics_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                        mime="text/csv"
                    )

                    if len(filtered_metrics) > 200:
                        st.warning(f"Showing first 200 of {len(filtered_metrics)} babies matching filters. Use more specific filters to narrow results.")

                else:
                    st.info("No babies match the selected filters.")
            else:
                st.info("No baby data available for the selected criteria.")
        else:
            st.info("No data available. Please check your filters and date range.")

if __name__ == "__main__":
    main()
