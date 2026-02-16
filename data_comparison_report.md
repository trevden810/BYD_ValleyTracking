# Data Source Comparison: Manual Export vs. API

## Executive Summary
**Current Status:** The **Manual Export (`scanfieldtest.xlsx`)** is significantly more accurate and comprehensive than the current API response.
**Recommendation:** 
1.  **Immediate:** Use the Manual Export to backfill/validate data for the Valley/BYD dashboards.
2.  **Confirmed Requirements:** The user has confirmed that `box_serial_numbers_scanned_received_json` is the primary source for scan data (Who, When, Where). "Build" fields are N/A.
2.  **Strategic:** Update the FileMaker Layout used by the API to include the missing fields identified below. This gives you the *accuracy* of the export with the *speed* of the API.

## Detailed Comparison

| Feature | Manual Export (`scanfieldtest.xlsx`) | Current API (High Level) | Impact |
| :--- | :--- | :--- | :--- |
| **Total Fields** | **~169 Columns** | **~43 Keys** | API misses ~75% of available data points. |
| **Product Detail** | ✅ `description_product`, `Dims_of_product`, `product_serial_number` | ⚠️ Limited (No Description) | **Critical**: Cannot identify *what* is being delivered via API. |
| **Timestamps** | ✅ `GEOFENCE_in/out`, `r_timestamp_delivered`, `time_arival`, `Build_Expected_DEL_Date` | ⚠️ Basic `job_date`, `time_complete` | **High**: Geofence data is more accurate than driver inputs. |
| **Valley Specifics** | ✅ `box_serial_numbers_scanned_received_json` | ❌ Missing | **Critical**: Validates who scanned the item and when. |
| **Notes** | ✅ Billing, Driver, Job, Schedule, All Read Only | ⚠️ Call Ahead, Driver Only | **Medium**: Missing context for billing/scheduling. |
| **Validation** | ✅ `box_serial_numbers_scanned` | ❌ Missing | **Medium**: Cannot validate specific item scans. |

## Key Missing Fields in API
To make the API as "accurate" as the manual export, the following fields need to be added to the FileMaker Layout (`Jobs`):

1.  **Product Identification:**
    -   `description_product` (Product Name/Description)
    -   `product_serial_number` (Specific Item ID)
2.  **Scan Validation (Who/When/Where):**
    -   `box_serial_numbers_scanned_received_json` (Contains JSON with `timestamp`, `username`, `manual`, `latitude`, `longitude`)
3.  **Note:**
    -   "Build" fields (`Build_Date_Built`, etc.) are N/A and **not required**.
    -   `box_serial_numbers_scanned_delivered_json` is **not required**.

## Conclusion
Manually exporting is currently the **better option for data accuracy**, specifically for the **Valley Tracking** project which likely relies on those "Build" dates and product descriptions. The current API data is insufficient for a full "Valley Tracking" dashboard.
