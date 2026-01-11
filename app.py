"""
GPS Driving Behavior Analyzer - Streamlit App
A web interface for analyzing GPS tracker data and generating driving behavior reports.
"""

import streamlit as st
import pandas as pd
import os
import sys
import tempfile
import zipfile
import io
from datetime import datetime
import traceback

# Page configuration
st.set_page_config(
    page_title="GPS Driving Analyzer",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1E88E5;
        text-align: center;
        margin-bottom: 2rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .success-box {
        padding: 1rem;
        background-color: #d4edda;
        border-radius: 0.5rem;
        border-left: 4px solid #28a745;
    }
    .info-box {
        padding: 1rem;
        background-color: #e7f3ff;
        border-radius: 0.5rem;
        border-left: 4px solid #1E88E5;
    }
    .warning-box {
        padding: 1rem;
        background-color: #fff3cd;
        border-radius: 0.5rem;
        border-left: 4px solid #ffc107;
    }
    .stProgress > div > div > div > div {
        background-color: #1E88E5;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'analysis_complete' not in st.session_state:
    st.session_state.analysis_complete = False
if 'output_files' not in st.session_state:
    st.session_state.output_files = []
if 'error_message' not in st.session_state:
    st.session_state.error_message = None


def validate_csv(df):
    """Validate that the uploaded CSV has required columns"""
    required_columns = ['dt_tracker', 'lat', 'lng', 'speed']
    optional_columns = ['dt_server', 'altitude', 'angle', 'params']
    
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        return False, f"Missing required columns: {', '.join(missing)}"
    
    # Check for valid data
    if df['lat'].isna().all() or df['lng'].isna().all():
        return False, "GPS coordinates (lat/lng) contain no valid data"
    
    if len(df) < 10:
        return False, "File contains too few data points (minimum 10 required)"
    
    return True, "Valid"


def create_download_zip(output_files, temp_dir):
    """Create a ZIP file containing all output files"""
    zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for file_path in output_files:
            if os.path.exists(file_path):
                arcname = os.path.basename(file_path)
                zip_file.write(file_path, arcname)
    
    zip_buffer.seek(0)
    return zip_buffer


def run_analysis(uploaded_files, driver_name, temp_dir):
    """Run the GPS analysis pipeline"""
    
    # Import the analyzer module (assumes it's in the same directory or PYTHONPATH)
    try:
        from gps_analyzer_phase2_pyeverywhere import analyze_gps_data
    except ImportError as e:
        st.error(f"❌ Failed to import analyzer module: {str(e)}")
        st.info("Make sure 'gps_analyzer_phase2_pyeverywhere.py' is in the app directory")
        return None, []
    
    # Save uploaded files to temp directory
    input_dir = os.path.join(temp_dir, "input")
    output_dir = os.path.join(temp_dir, "output")
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    
    csv_paths = []
    for uploaded_file in uploaded_files:
        file_path = os.path.join(input_dir, uploaded_file.name)
        with open(file_path, 'wb') as f:
            f.write(uploaded_file.getbuffer())
        csv_paths.append(file_path)
    
    # Determine input format
    if len(csv_paths) == 1:
        input_data = csv_paths[0]
        base_name = os.path.splitext(os.path.basename(csv_paths[0]))[0]
    else:
        input_data = csv_paths
        base_name = "multi_week_analysis"
    
    output_prefix = os.path.join(output_dir, f"{base_name}_analysis")
    
    # Run analysis
    try:
        result = analyze_gps_data(
            csv_files_or_pattern=input_data,
            driver_name=driver_name,
            output_prefix=output_prefix,
            generate_excel=True
        )
        
        # Collect output files
        output_files = []
        for root, dirs, files in os.walk(output_dir):
            for file in files:
                output_files.append(os.path.join(root, file))
        
        # Also check for files generated in temp_dir root
        for file in os.listdir(temp_dir):
            file_path = os.path.join(temp_dir, file)
            if os.path.isfile(file_path) and file_path not in output_files:
                if file.endswith(('.xlsx', '.html', '.csv', '.png', '.pdf')):
                    output_files.append(file_path)
        
        return result, output_files
        
    except Exception as e:
        st.error(f"❌ Analysis failed: {str(e)}")
        st.code(traceback.format_exc())
        return None, []


# Main UI
st.markdown('<p class="main-header">🚗 GPS Driving Behavior Analyzer</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Upload your tracker CSV files and get detailed driving behavior reports</p>', unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.header("⚙️ Settings")
    
    driver_name = st.text_input(
        "Driver Name",
        value="Driver",
        help="Name to appear on the report"
    )
    
    st.divider()
    
    st.header("📋 CSV Format")
    st.markdown("""
    **Required columns:**
    - `dt_tracker` - Timestamp
    - `lat` - Latitude  
    - `lng` - Longitude
    - `speed` - Speed (km/h)
    
    **Optional columns:**
    - `dt_server` - Server timestamp
    - `altitude` - Altitude (m)
    - `angle` - Heading angle
    - `params` - Tracker parameters
    """)
    
    st.divider()
    
    st.header("ℹ️ About")
    st.markdown("""
    This analyzer processes GPS tracker data to evaluate:
    - 🔄 Roundabout navigation
    - 🛑 Stop sign compliance  
    - 🚦 Traffic light behavior
    - ⚡ Speed zone management
    - 💥 Harsh driving events
    """)

# Main content area
col1, col2 = st.columns([2, 1])

with col1:
    st.header("📁 Upload GPS Data")
    
    uploaded_files = st.file_uploader(
        "Choose CSV file(s)",
        type=['csv'],
        accept_multiple_files=True,
        help="Upload one or more CSV files from your GPS tracker"
    )
    
    if uploaded_files:
        st.success(f"✅ {len(uploaded_files)} file(s) uploaded")
        
        # Preview data
        with st.expander("👀 Preview uploaded data", expanded=False):
            for uploaded_file in uploaded_files:
                st.subheader(f"📄 {uploaded_file.name}")
                try:
                    df = pd.read_csv(uploaded_file)
                    uploaded_file.seek(0)  # Reset file pointer
                    
                    # Validate
                    is_valid, message = validate_csv(df)
                    if is_valid:
                        st.markdown(f"✅ **Valid format** - {len(df)} data points")
                    else:
                        st.error(f"❌ {message}")
                    
                    st.dataframe(df.head(10), use_container_width=True)
                    
                except Exception as e:
                    st.error(f"Error reading file: {str(e)}")

with col2:
    st.header("📊 Analysis Status")
    
    if st.session_state.analysis_complete:
        st.markdown('<div class="success-box">✅ Analysis Complete!</div>', unsafe_allow_html=True)
    elif st.session_state.error_message:
        st.error(st.session_state.error_message)
    else:
        st.markdown('<div class="info-box">📤 Upload files to begin</div>', unsafe_allow_html=True)

# Analysis button
st.divider()

col_btn1, col_btn2, col_btn3 = st.columns([1, 2, 1])

with col_btn2:
    analyze_button = st.button(
        "🚀 Analyze Driving Behavior",
        use_container_width=True,
        type="primary",
        disabled=not uploaded_files
    )

# Run analysis
if analyze_button and uploaded_files:
    st.session_state.analysis_complete = False
    st.session_state.output_files = []
    st.session_state.error_message = None
    
    # Validate all files first
    all_valid = True
    for uploaded_file in uploaded_files:
        try:
            df = pd.read_csv(uploaded_file)
            uploaded_file.seek(0)
            is_valid, message = validate_csv(df)
            if not is_valid:
                st.error(f"❌ {uploaded_file.name}: {message}")
                all_valid = False
        except Exception as e:
            st.error(f"❌ {uploaded_file.name}: Could not read file - {str(e)}")
            all_valid = False
    
    if all_valid:
        with st.spinner("🔄 Analyzing GPS data... This may take a few minutes..."):
            # Create progress indicators
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Create temp directory
            with tempfile.TemporaryDirectory() as temp_dir:
                status_text.text("📂 Preparing files...")
                progress_bar.progress(10)
                
                status_text.text("🗺️ Loading map data...")
                progress_bar.progress(20)
                
                status_text.text("📍 Processing GPS points...")
                progress_bar.progress(40)
                
                # Run actual analysis
                result, output_files = run_analysis(uploaded_files, driver_name, temp_dir)
                
                progress_bar.progress(80)
                status_text.text("📊 Generating reports...")
                
                if result and output_files:
                    progress_bar.progress(100)
                    status_text.text("✅ Analysis complete!")
                    
                    st.session_state.analysis_complete = True
                    st.session_state.output_files = output_files
                    
                    # Create download section
                    st.divider()
                    st.header("📥 Download Results")
                    
                    # Create ZIP of all files
                    if output_files:
                        zip_buffer = create_download_zip(output_files, temp_dir)
                        
                        col_dl1, col_dl2 = st.columns([1, 1])
                        
                        with col_dl1:
                            st.download_button(
                                label="📦 Download All Results (ZIP)",
                                data=zip_buffer,
                                file_name=f"gps_analysis_{driver_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
                                mime="application/zip",
                                use_container_width=True
                            )
                        
                        # Individual file downloads
                        st.subheader("📄 Individual Files")
                        for file_path in output_files:
                            file_name = os.path.basename(file_path)
                            with open(file_path, 'rb') as f:
                                file_data = f.read()
                            
                            # Determine mime type
                            if file_name.endswith('.xlsx'):
                                mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                icon = "📊"
                            elif file_name.endswith('.html'):
                                mime = "text/html"
                                icon = "🗺️"
                            elif file_name.endswith('.csv'):
                                mime = "text/csv"
                                icon = "📋"
                            elif file_name.endswith('.png'):
                                mime = "image/png"
                                icon = "🖼️"
                            else:
                                mime = "application/octet-stream"
                                icon = "📄"
                            
                            st.download_button(
                                label=f"{icon} {file_name}",
                                data=file_data,
                                file_name=file_name,
                                mime=mime,
                                key=f"dl_{file_name}"
                            )
                        
                        st.balloons()
                else:
                    st.session_state.error_message = "Analysis completed but no output files were generated"
                    progress_bar.progress(100)
                    status_text.text("⚠️ Analysis completed with warnings")

# Footer
st.divider()
st.markdown("""
<div style="text-align: center; color: #888; font-size: 0.8rem;">
    GPS Driving Behavior Analyzer v1.0 | Built with Streamlit
</div>
""", unsafe_allow_html=True)
