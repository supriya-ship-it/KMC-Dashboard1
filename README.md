# KMC Dashboard

A comprehensive Streamlit dashboard for monitoring Kangaroo Mother Care (KMC) program across hospitals.

## Features

- **Program Overview**: Total babies, active cases, and hospital distribution
- **Clinical KPIs**: Registration timeliness, KMC initiation timing, follow-up completion, discharge outcomes
- **Mortality Analysis**: Comprehensive death rate analysis with multiple breakdowns
- **Daily KMC Analysis**: Real-time monitoring of KMC hours by location and hospital
- **Data Explorer**: Detailed baby records with search and filter capabilities

## Technical Details

- **Framework**: Streamlit
- **Database**: Firebase Firestore
- **Data Collections**: `baby`, `babyBackUp`, `discharges`
- **Visualization**: Plotly charts and graphs

## Installation

1. Clone this repository
2. Install required dependencies:
   ```bash
   pip install streamlit pandas numpy plotly firebase-admin
   ```
3. Add your `firebase-key.json` file to the project root
4. Run the dashboard:
   ```bash
   streamlit run kmc_dashboard.py
   ```

## Usage

The dashboard automatically loads data from Firebase collections and provides real-time KPI monitoring. Use the sidebar filters to narrow down analysis by hospital, date range, or specific UIDs.

## Key Metrics

### Clinical KPIs
- **Registration Timeliness**: Tracks inborn baby registration within 12/24 hours
- **KMC Initiation**: Monitors time to first KMC session
- **Follow-up Completion**: Tracks completion of 2, 7, 14, and 28-day follow-ups
- **Discharge Outcomes**: Categorizes discharge status and outcomes

### Mortality Analysis
- **Overall Mortality Rate**: Based on `deadBaby = true` field
- **Hospital-wise Analysis**: Mortality rates by hospital
- **Demographics**: Inborn vs outborn mortality comparison
- **Location Analysis**: Mortality by baby location
- **KMC Stability**: Mortality correlation with KMC stability

## Data Sources

- **Baby Collection**: Primary baby records with observation data
- **BabyBackUp Collection**: Historical baby records
- **Discharges Collection**: Discharge status and outcomes

## Deployment

This dashboard is deployed on Streamlit Cloud for public access.

## Version History

See git commits for detailed version history and changes.
