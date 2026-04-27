# Dataset README

## Folder Structure

Each subject has the following directory structure:

```
P00X/
├── EEG/
│ ├── sub-P00X_ses-S001_task_T1_run-001_eeg.xdf
│ ├── sub-P00X_ses-S001_task_T2_acq-XXX_MX_run-001_eeg.xdf
│ ├── sub-P00X_ses-S001_task_T3_acq-XXX_MX_run-001_eeg.xdf
│
├── Comfort/
│ ├── P00X_mean_absolute.csv
│ ├── P00X_relative.csv
│ ├── P00X_single_absolute.csv
│
├── Movements/
│ ├── P00X_movements_XXXX_MX.csv
│ ├── P00X_movements_XXXX_MX.mp4
│ ├── P00X_movements_XXXX_MX.csv
│ ├── P00X_movements_XXXX_MX.mp4
```

---

## EEG Folder

Each `.xdf` file contains:

- g.USBamp setup information (channel names, sampling rate)
- EEG recordings
- Unity markers time-locked to EEG data

### File Types

- **`task_T1_run-001_eeg`**
  - Data recorded during the stimulus presentation pipeline

- **`task_T2/3_acq-XXXX_MX_run-001_eeg`**
  - Data recorded during the SSVEP game

### Acquisition Naming Convention

- `M1`, `M2` → Map used during gameplay (Map 1 or Map 2)
- `acq-BW` → BW = Standard stimuli
- `acq-CXSX` → CXSX = Personal stimuli  (corresponding to specific Contrast X and Size X settings)

---

## Comfort Folder

### `P00X_single_absolute.csv`

Contains comfort ratings per stimulus in the following structure:

| Stimulus Contrast| Stimulus Size | Epoch Number | Comfort Score |

* Epoch number is included becasue each stimulus was presented more than one time, and each viewing corresponds to a unique comfort rating. 
---

### `P00X_mean_absolute.csv`

- Average comfort ratings across epochs
- 12 columns → one per stimulus

---

### `P00X_relative.csv`

Pairwise comparison results:

| Pair Number | 1st stimulus | 2nd stimulus | Selected stimulus |

* For each pair, the `1st stimulus` and `2nd stimulus columns` contain the names of the stimuli presented 1st and 2nd in the pair, respectively, and the `Selected stimulus` column  contains the name of the stimulus that the participant chose as more comfortable.

---

## Movements Folder

Each file corresponds to one gameplay session.

### File Types

- `.csv` → Movement data
- `.mp4` → Video visualization of movement

### Naming Convention

- `M1`, `M2` → Map used during gameplay (Map 1 or Map 2)
-  `BW` → BW = Standard stimuli
- `CXSX` → CXSX = Personal stimuli  (corresponding to specific Contrast X and Size X settings)

---

### ⚠️ Known Issue (Participants P001–P009)

- Failed movements do **not record intended movement direction**
- Reflected as:
  - Empty cells in `.csv` files
  - Special icon in movement videos (See `Movement_Video_Legend.pdf` for details.)

---

## Repository-Level Files

| File Name | Description |
|----------|-------------|
| `Movement_Video_Legend.pdf` | Detailed description of colors and symbols used in movement videos. |
| `Map1.png` | Visual of Map 1. |
| `Map2.png` | Visual of Map 2. |
| `Participant_Demographic_Info.csv` | Demographic information for all participants including age and sex. |
| `NASA-TLX.pdf` | NASA-TLX survey form. |
| `NASA-TLX-Standard-Responses.csv` | Participant responses on the NASA-TLX survey in reference to playing the SSVEP game with Standard stimuli.|
| `NASA-TLX-Personal-Responses.csv` | Participant responses on the NASA-TLX survey in reference to playing the SSVEP game with Personal stimuli. |
| `SSVEP-Survey.pdf` | Survey given to participants with questions. regarding their experience using Personal and Standard SSVEP stimuli. |
| `SSVEP-Survey-Responses.csv` | Participant responses to SSVEP survey questions. |
| `epoching-example.ipynb` | Jupyter Notebook showing an example of how to import and epoch the provided `xdf` files. |
| `Notes.docx` | Notes on movement issues, artifacts, incomplete data. |

---

## Notes

- EEG data includes synchronized event markers for analysis.
- Movement videos provide visual context for gameplay behavior.
- Refer to `Notes.docx` for participant-specific data quality issues.

---