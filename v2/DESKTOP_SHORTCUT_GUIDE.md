# Desktop Shortcut Setup Guide

## Quick Instructions

### Create Desktop Shortcut

1. **Right-click on your Desktop** → **New** → **Shortcut**

2. **Enter the Target Location**:
   ```
   C:\Projects\BYD_ValleyTracking\run_daily_import.bat
   ```
   OR (for quiet mode without dashboard):
   ```
   C:\Projects\BYD_ValleyTracking\run_daily_import_quiet.bat
   ```

3. **Click Next**, then enter a name:
   ```
   BYD Valley Daily Import
   ```

4. **Click Finish**

### Customize the Shortcut (Optional)

1. **Right-click the shortcut** → **Properties**

2. **Change Icon** (Optional):
   - Click **Change Icon**
   - Browse to `C:\Windows\System32\imageres.dll`
   - Select an icon (e.g., #147 for a folder with an arrow, #2 for a monitor)

3. **Set Working Directory** (Important):
   - In the **Start in** field, enter:
     ```
     C:\Projects\BYD_ValleyTracking
     ```

4. **Run as Administrator** (If needed):
   - Click **Advanced**
   - Check **Run as administrator** (only if required)
   - Click **OK**

5. **Click Apply**, then **OK**

## Usage

### With Dashboard (Recommended for Daily Use)
Double-click **BYD Valley Daily Import** shortcut:
- ✓ Finds latest export from OneDrive
- ✓ Processes data through V2.0 pipeline
- ✓ Sends/displays email report
- ✓ Stores data in Supabase
- ✓ Launches Streamlit dashboard in your browser

### Quiet Mode (Data Processing Only)
Use the `run_daily_import_quiet.bat` shortcut:
- ✓ Finds latest export from OneDrive
- ✓ Processes data and sends email
- ✗ Does NOT launch dashboard
- ✓ Shows summary and waits for you to close

## Troubleshooting

### Shortcut doesn't work
- Ensure the **Start in** directory is set to `C:\Projects\BYD_ValleyTracking`
- Verify Python is installed and in your PATH
- Check that the virtual environment exists (if you're using one)

### "No export files found" error
- Verify the OneDrive directory path is correct
- Check that export files exist in the expected format (`MM_DD_YY.NN.xlsx`)
- Ensure OneDrive is synced and the files are downloaded locally

### Python/module not found errors
- Activate your virtual environment manually:
  ```powershell
  cd C:\Projects\BYD_ValleyTracking
  .\venv\Scripts\activate
  pip install -r requirements.txt
  pip install -r v2\requirements_v2.txt
  ```

## Advanced: Windows Task Scheduler

To run this automatically every morning:

1. Open **Task Scheduler** (search in Start menu)

2. Click **Create Basic Task**

3. Name: `BYD Valley Daily Import`

4. Trigger: **Daily** at **6:00 AM** (or your preferred time)

5. Action: **Start a program**
   - Program/script: `C:\Projects\BYD_ValleyTracking\run_daily_import_quiet.bat`
   - Start in: `C:\Projects\BYD_ValleyTracking`

6. Finish and **test** the task by right-clicking → **Run**

> [!TIP]
> Use the "quiet" version for scheduled tasks so it doesn't launch the dashboard automatically every morning.
