# SP/OneDrive Copy Tool (GUI)

A simple, resilient GUI tool to mirror folders and files from SharePoint/OneDrive (source) to SharePoint/OneDrive (destination) via Microsoft Graph (app-only). It supports resumable uploads, checkpoint/resume with per-job state, and a clean Tkinter UI.

## Features
- Mirror **folder structure** first, then **files** (two-phase)
- **Resumable** large file uploads (Graph upload sessions)
- **Checkpoint/Resume** between runs (per-job `.state` under user profile)
- **Cancel-safe**: stops after the current step; keeps progress
- **Audit pass**: size-only verification (strict hash mode optional later)

## Requirements
- Python **3.10+**
- A Microsoft Entra (Azure AD) app registration with **client secret** (app-only)
- Graph **Application** permissions (common set):
  - `Files.ReadWrite.All`
  - `Sites.Read.All`
  - `User.Read.All` (for OneDrive user search)
  - (Optionally `Sites.ReadWrite.All` if you need broader write)
- Admin consent granted for the tenant.

> The app uses app-only (client credentials) flow via `msal`.

## Install (dev)
```bash
# Clone
git clone https://github.com/yourname/SimplePythonSPorOneDriveMoveTool.git
cd SimplePythonSPorOneDriveMoveTool

# Create and activate a venv
python -m venv .venv
# Windows
. .venv/Scripts/activate
# macOS/Linux
# source .venv/bin/activate

# Install (editable)
pip install -e .


**USAGE**

![NVIDIA_Overlay_cqUo8DI9G8](https://github.com/user-attachments/assets/db8f2eaa-fa61-423a-8849-06a4013ff351)

## Usage (30s)

1) **Run**
   ```bash
   python main.py
Connect

Enter TENANT, CLIENT (App ID), SECRET → click Connect.

Pick Source
Site → Library → Parent folder.
Pick Destination
Choose SharePoint or OneDrive (via the radio buttons).

SharePoint: Site → Library → Parent.
OneDrive: Search user → select user → root/parent.

(Optional) ROOT_NAME

Set the top-level folder name at the destination.

Start / Monitor / Cancel
Click Start to begin.
Watch Stats (files, rate, elapsed, ETA, workers, throttles) and Output.
Cancel anytime; progress is checkpointed in .state/.

Notes
Requires Graph app-only permissions (e.g., Files.ReadWrite.All, Sites.Read.All, User.Read.All).
.state/ holds transient job data and is ignored by git.
