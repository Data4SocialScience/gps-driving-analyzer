# 🚗 GPS Driving Behavior Analyzer

A Streamlit web application for analyzing GPS tracker data and generating comprehensive driving behavior reports.

## Features

- 📤 **Easy Upload**: Drag and drop CSV files from your GPS tracker
- 📊 **Comprehensive Analysis**: Evaluates roundabout navigation, stop sign compliance, traffic lights, speed zones, and harsh driving events
- 📥 **Downloadable Reports**: Get Excel reports, HTML maps, and verification sheets
- 🎨 **Clean Interface**: Simple, user-friendly design for non-technical users

## Quick Start

### Option 1: Local Setup (Recommended for Testing)

1. **Clone/Download the repository**
   ```bash
   git clone <your-repo-url>
   cd streamlit_gps_app
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Add your analyzer code**
   - Copy `gps_analyzer_phase2_pyeverywhere.py` to this directory
   - Copy your `map_cache` folder with `alsace_master_cache.pkl`

5. **Run the app**
   ```bash
   streamlit run app.py
   ```

6. **Open browser** at `http://localhost:8501`

### Option 2: Deploy to Streamlit Cloud

1. **Push to GitHub**
   ```bash
   git init
   git add .
   git commit -m "Initial commit"
   git remote add origin <your-github-repo>
   git push -u origin main
   ```

2. **Required files in repository:**
   ```
   streamlit_gps_app/
   ├── app.py                              # Main Streamlit app
   ├── gps_analyzer_phase2_pyeverywhere.py # Your analyzer code
   ├── requirements.txt                    # Python dependencies
   ├── packages.txt                        # System dependencies
   ├── map_cache/                          # Map cache folder
   │   └── alsace_master_cache.pkl         # Pre-built cache (REQUIRED!)
   └── .streamlit/
       └── config.toml                     # Theme configuration
   ```

3. **Deploy on Streamlit Cloud**
   - Go to [share.streamlit.io](https://share.streamlit.io)
   - Connect your GitHub account
   - Select your repository
   - Set main file path: `app.py`
   - Click "Deploy"

## ⚠️ Important Notes

### Master Cache Requirement

The analyzer requires a pre-built master cache file (`alsace_master_cache.pkl`). This file must be:
- Built locally first using your analyzer code
- Included in the deployment
- Located in the `map_cache/` directory

**To build the cache locally:**
```python
from full_code_gps_analyzer_phase2_v1 import UnifiedOfflineMapDataManager

manager = UnifiedOfflineMapDataManager(
    pbf_file_path="alsace-latest.osm.pbf"  # Download from Geofabrik
)
manager.build_master_cache()
```

### File Size Considerations

- The master cache file can be 50-200 MB
- Streamlit Cloud has a 1GB app size limit
- Consider using Git LFS for large files:
  ```bash
  git lfs install
  git lfs track "*.pkl"
  git add .gitattributes
  ```

## CSV Input Format

Your GPS tracker CSV files should have these columns:

| Column | Required | Description |
|--------|----------|-------------|
| `dt_tracker` | ✅ | Timestamp from tracker |
| `lat` | ✅ | Latitude |
| `lng` | ✅ | Longitude |
| `speed` | ✅ | Speed in km/h |
| `dt_server` | ❌ | Server timestamp |
| `altitude` | ❌ | Altitude in meters |
| `angle` | ❌ | Heading angle |
| `params` | ❌ | Additional parameters |

## Troubleshooting

### "Failed to import analyzer module"
- Ensure `gps_analyzer_phase2_pyeverywhere.py` is in the app directory
- Check Python path settings

### "Master cache not found"
- Build the cache locally first
- Ensure `map_cache/alsace_master_cache.pkl` exists

### Memory errors on Streamlit Cloud
- Streamlit Cloud has 1GB memory limit
- Consider using a paid tier or local deployment for large analyses

### Slow performance
- First analysis after deployment is slower (cache loading)
- Subsequent analyses are faster

## File Structure

```
streamlit_gps_app/
├── app.py                    # Main Streamlit application
├── requirements.txt          # Python dependencies
├── packages.txt              # System dependencies (for Cloud)
├── README.md                 # This file
├── .streamlit/
│   └── config.toml          # Streamlit configuration
├── gps_analyzer_phase2_pyeverywhere.py  # Your analyzer (add this)
└── map_cache/                           # Cache directory (add this)
    └── alsace_master_cache.pkl          # Pre-built cache (add this)
```

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review Streamlit Cloud logs
3. Contact the developer

---

Built with ❤️ using [Streamlit](https://streamlit.io)
