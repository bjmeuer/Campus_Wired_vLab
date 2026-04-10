#!/usr/bin/env python3
"""
CloudVision Studio Input Updater

Uploads or updates the inputs of a CloudVision Studio using the
CloudVision Resource API (studio/v1/Inputs).

The input file should be YAML or JSON with the structure:
    path: []                # path within the studio input tree ([] = root)
    inputs:                 # the studio input data
      someKey: someValue
      ...

Usage examples:
    # With an API token (recommended):
    python cvp_studio_update.py \\
        --host cvp.example.com \\
        --token <api_token> \\
        --input-folder studio_inputs/ \\
        --workspace-id <workspace_id>

    # With username/password, creating a new workspace:
    python cvp_studio_update.py \\
        --host cvp.example.com \\
        --username admin --password secret \\
        --input-folder studio_inputs/ \\
        --new-workspace "my-update-workspace" \\
        --build --submit
"""

import argparse
import datetime
import json
import os
import sys
import uuid
import time

import requests
import urllib3
from pathlib import Path

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

# Suppress SSL warnings when verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Workspace ID used to read committed (mainline) state from the Resource API
MAINLINE_WORKSPACE_ID = ""


class CVPClient:
    """Minimal CloudVision REST/Resource API client."""

    def __init__(self, host, token=None, username=None, password=None,
                 verify_ssl=True, port=443):
        self.base_url = f"https://{host}:{port}"
        self.session = requests.Session()
        self.session.verify = verify_ssl

        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"
        elif username and password:
            self._login(username, password)
        else:
            raise ValueError("Provide --token or both --username and --password.")

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def _login(self, username, password):
        """Authenticate with CVP username/password and store the session cookie."""
        url = f"{self.base_url}/web/login/authenticate.do"
        resp = self.session.post(url, json={"userId": username, "password": password})
        resp.raise_for_status()
        data = resp.json()
        if "sessionId" not in data:
            raise RuntimeError(f"Login failed: {data.get('errorMessage', data)}")
        # Session cookie is handled automatically by requests.Session

    # ------------------------------------------------------------------
    # Resource API helpers
    # ------------------------------------------------------------------

    def _resource_url(self, path: str) -> str:
        return f"{self.base_url}/api/resources/{path}"

    @staticmethod
    def _raise_for_status(resp: requests.Response) -> None:
        """Like raise_for_status() but includes the response body in the message."""
        if resp.ok:
            return
        try:
            detail = resp.json()
        except ValueError:
            detail = resp.text[:500]
        raise requests.HTTPError(
            f"HTTP {resp.status_code} {resp.reason} — {detail}",
            response=resp,
        )

    def _get(self, resource_path: str, params: dict = None) -> dict:
        resp = self.session.get(self._resource_url(resource_path), params=params)
        self._raise_for_status(resp)
        return resp.json()

    def _get_all(self, resource_path: str) -> list:
        """
        Fetch a streaming ``/all`` endpoint (no filter) via GET.
        Returns a flat list of the ``value`` objects from each NDJSON line.
        """
        resp = self.session.get(self._resource_url(resource_path))
        self._raise_for_status(resp)
        return self._parse_ndjson(resp.text)

    def _post_all(self, resource_path: str, payload: dict = None) -> list:
        """
        Fetch a streaming ``/all`` endpoint with a ``partialEqFilter`` body via POST.
        Returns a flat list of the ``value`` objects from each NDJSON line.
        """
        resp = self.session.post(self._resource_url(resource_path), json=payload or {})
        self._raise_for_status(resp)
        return self._parse_ndjson(resp.text)

    @staticmethod
    def _parse_ndjson(text: str) -> list:
        results = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            value = obj.get("result", {}).get("value")
            if value:
                results.append(value)
        return results

    def _post(self, resource_path: str, payload: dict) -> dict:
        resp = self.session.post(self._resource_url(resource_path), json=payload)
        self._raise_for_status(resp)
        return resp.json()

    def _put(self, resource_path: str, payload: dict) -> dict:
        resp = self.session.put(self._resource_url(resource_path), json=payload)
        self._raise_for_status(resp)
        return resp.json()

    # ------------------------------------------------------------------
    # Workspace operations
    # ------------------------------------------------------------------

    def list_workspaces(self) -> list:
        """Return a list of all workspace objects."""
        return self._get_all("workspace/v1/Workspace/all")

    def create_workspace(self, display_name: str, description: str = "") -> str:
        """Create a new workspace and return its ID.

        Uses the writable WorkspaceConfig resource (the Workspace resource is
        the read-only state counterpart in the CVaaS Resource API).
        """
        workspace_id = str(uuid.uuid4())
        self._post("workspace/v1/WorkspaceConfig", {
            "key": {"workspaceId": workspace_id},
            "displayName": display_name,
            "description": description,
        })
        return workspace_id

    def build_submit_workspace(self, workspace_id: str, request: int) -> dict:
        """Submit the workspace to mainline (approve & apply)."""
        payload = {
            "key": {
                "workspaceId": workspace_id,
            },
            "request": request,
            "requestParams":{
                "requestId":"request_" + str(uuid.uuid4())
            },
        }
        return self._post("workspace/v1/WorkspaceConfig", payload)

    # ------------------------------------------------------------------
    # Studio operations
    # ------------------------------------------------------------------

    def list_studios(self) -> list:
        """Return all Studio objects from the mainline workspace."""
        return self._get_all("studio/v1/Studio/all")

    def find_studio_by_name(self, display_name: str) -> str:
        """
        Look up a Studio by its displayName (case-insensitive).

        The API may return the same studio more than once; a set is used to
        collect unique studioIds so duplicate entries don't cause false errors.

        Returns the studioId string, or raises RuntimeError if not found
        or if the name matches studios with genuinely different IDs.
        """
        studios = self._post_all("studio/v1/Studio/all", {"partialEqFilter": [{"displayName": display_name}],})
        matched_ids = {
            s["key"]["studioId"]
            for s in studios
            if s.get("displayName", "").lower() == display_name.lower()
        }
        if not matched_ids:
            available = sorted({s.get("displayName", "<unnamed>") for s in studios})
            raise RuntimeError(
                f"No studio with displayName '{display_name}' found.\n"
                f"Available studios: {available}"
            )
        if len(matched_ids) > 1:
            raise RuntimeError(
                f"Multiple distinct studios named '{display_name}': {matched_ids}. "
                "Use --studio-id to disambiguate."
            )
        return matched_ids.pop()

    # ------------------------------------------------------------------
    # Studio Input operations
    # ------------------------------------------------------------------

    def get_studio_inputs(self, workspace_id: str, studio_id: str,
                          path: list = None) -> dict:
        """Retrieve the current studio inputs at the given path."""
        path = path or []
        params = {
            "key.workspaceId": workspace_id,
            "key.studioId": studio_id,
            "key.path.values": path,
        }
        return self._get("studio/v1/Inputs", params=params)
    
    def convert_yaml_to_cv_json(self, yaml_file, workspace_id, studio_id):
        # 1. Load your exported YAML
        with open(yaml_file, 'r') as f:
            exported_data = yaml.safe_load(f)

        return json.dumps(exported_data)

   

    def set_studio_inputs(self, workspace_id: str, studio_id: str,
                          inputs: dict, path: list = None) -> dict:
        """
        Set (create or replace) studio inputs at the given path.

        Parameters
        ----------
        workspace_id : ID of the workspace to update
        studio_id    : ID of the studio to update
        inputs       : dict of input data (will be JSON-encoded for the API)
        path         : list of path elements; [] means root
        """
        path = path or []
        payload = {
            "key": {
                "workspaceId": workspace_id,
                "studioId": studio_id,
                "path": {"values": path},
            },
            "inputs": json.dumps(inputs),
        }
        # Use PUT if the resource already exists, POST otherwise.
        # The CVP Resource API accepts POST for both create and replace.
        try:
            return self._post("studio/v1/InputsConfig", payload)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 405:
                # Fall back to PUT
                return self._put("studio/v1/InputsConfig", payload)
            raise


# ----------------------------------------------------------------------
# File loading
# ----------------------------------------------------------------------

def load_input_file(file_path: str) -> tuple[list, dict]:
    """
    Load a studio input file (YAML or JSON).

    Returns
    -------
    path   : list of path elements (may be empty for root)
    inputs : dict of input data
    """
    with open(file_path, "r", encoding="utf-8") as fh:
        content = fh.read()

    if file_path.lower().endswith((".yaml", ".yml")):
        if not HAS_YAML:
            sys.exit("PyYAML is required for YAML files: pip install pyyaml")
        data = yaml.safe_load(content)
    else:
        data = json.loads(content)

    if not isinstance(data, dict):
        sys.exit(f"Input file must contain a mapping at the top level, got {type(data).__name__}")

    path = data.get("path", [])
    inputs = data.get("inputs")

    if inputs is None:
        sys.exit("Input file must have an 'inputs' key.")
    if not isinstance(path, list):
        sys.exit("The 'path' key must be a list (e.g. [] for root).")

    return path, inputs


# ----------------------------------------------------------------------
# Backup helpers
# ----------------------------------------------------------------------

def save_backup(client: "CVPClient", studio_id: str, path: list,
                input_file: str, backup_dir: str) -> str | None:
    """
    Fetch the current studio inputs from the mainline workspace and write
    them to *backup_dir* using the same filename convention as the source
    file, with a ``_YYYYMMDD_HHMMSS`` timestamp appended before the extension.

    Uses the /all streaming endpoint to avoid the empty-path query-param
    serialization issue (an empty repeated field is omitted by requests,
    causing the server to see ``path = nil``).

    Returns the path of the saved file, or None if mainline has no inputs yet.
    """
    # InputsService.GetAll — POST to the InputsConfig /all endpoint (streaming NDJSON)
    items = client._post_all("studio/v1/Inputs/all", {
        "partialEqFilter": [{"key": {"studioId": studio_id, "workspaceId": MAINLINE_WORKSPACE_ID}}],
    })
    match = next(
        (i for i in items
         if i.get("key", {}).get("path", {}).get("values", []) == path),
        None,
    )
    if not match:
        return None

    raw = match.get("inputs", "{}")
    current_inputs = json.loads(raw) if isinstance(raw, str) else raw

    backup_data = {"path": path, "inputs": current_inputs}

    basename = os.path.basename(input_file)
    name, ext = os.path.splitext(basename)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"{name}_{timestamp}{ext}"

    os.makedirs(backup_dir, exist_ok=True)
    backup_path = os.path.join(backup_dir, backup_filename)

    with open(backup_path, "w", encoding="utf-8") as fh:
        if ext.lower() in (".yaml", ".yml") and HAS_YAML:
            yaml.dump(backup_data, fh, default_flow_style=False, allow_unicode=True)
        else:
            json.dump(backup_data, fh, indent=2)

    return backup_path


# ----------------------------------------------------------------------
# Studio name helpers
# ----------------------------------------------------------------------

def studio_name_from_filename(file_path: str) -> str | None:
    """
    Derive a Studio displayName from the input filename convention
    ``Inputs_<DisplayName>.<ext>``.

    Returns the display name string, or None if the filename does not
    follow the expected pattern.
    """
    basename = os.path.splitext(os.path.basename(file_path))[0]
    prefix = "Inputs_"
    if basename.startswith(prefix):
        return str(basename[len(prefix):]).replace("_","/")
    return None


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Upload/update a CloudVision Studio's inputs from a file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Connection
    conn = parser.add_argument_group("Connection")
    conn.add_argument("--host", required=True, metavar="HOST",
                      help="CVP hostname or IP address")
    conn.add_argument("--port", type=int, default=443, metavar="PORT",
                      help="HTTPS port (default: 443)")
    conn.add_argument("--no-verify-ssl", action="store_true",
                      help="Disable SSL certificate verification")

    # Authentication (mutually exclusive)
    auth = parser.add_argument_group("Authentication (choose one)")
    auth_ex = auth.add_mutually_exclusive_group(required=True)
    auth_ex.add_argument("--token", metavar="TOKEN",
                         help="CVP service account or user API token")
    auth_ex.add_argument("--username", metavar="USER",
                         help="CVP username (requires --password)")
    auth.add_argument("--password", metavar="PASS",
                      help="CVP password (used with --username)")

    # Studio
    studio = parser.add_argument_group("Studio")
    studio.add_argument("--input-folder", default="../studio_inputs", metavar="FILE",
                        help="YAML or JSON file containing the studio inputs")

    # Workspace
    ws = parser.add_argument_group("Workspace")
    ws_ex = ws.add_mutually_exclusive_group()
    ws_ex.add_argument("--workspace-id", metavar="WS_ID",
                       help="Existing workspace ID to update")
    ws_ex.add_argument("--new-workspace", metavar="NAME",
                       help="Create a new workspace with this display name")

    # Backup
    backup = parser.add_argument_group("Backup")
    backup.add_argument("--no-backup", dest="backup", action="store_false",
                        help="Skip backing up current studio inputs before updating")
    backup.add_argument("--backup-dir", metavar="DIR", default="../studio_inputs_backup",
                        help="Directory for backup files (default: studio_inputs_backup)")
    parser.set_defaults(backup=True)

    # Actions
    actions = parser.add_argument_group("Post-update actions")
    actions.add_argument("--build", action="store_true",
                         help="Build the workspace after updating inputs")
    actions.add_argument("--submit", action="store_true",
                         help="Submit the workspace after building (implies --build)")

    # Misc
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Print detailed progress")

    return parser


def log(msg: str, verbose: bool = False, always: bool = False):
    if always or verbose:
        print(msg)


def main():
    parser = build_parser()
    args = parser.parse_args()

    # Validate auth combination
    if args.username and not args.password:
        parser.error("--password is required when using --username")

    if args.submit:
        args.build = True

    verify_ssl = not args.no_verify_ssl
    
    # ------------------------------------------------------------------
    # Connect to CVP
    # ------------------------------------------------------------------
    log(f"\nConnecting to CVP: https://{args.host}:{args.port}", args.verbose, always=True)
    client = CVPClient(
        host=args.host,
        port=args.port,
        token=args.token,
        username=args.username,
        password=args.password,
        verify_ssl=verify_ssl,
    )
    log("  Connected.", args.verbose)
    
     # ------------------------------------------------------------------
    # Create or Resolve workspace
    # ------------------------------------------------------------------
    if args.workspace_id:
        workspace_id = args.workspace_id
        log(f"\nUsing existing workspace: {workspace_id}", args.verbose, always=True)
    elif args.new_workspace:
        log(f"\nCreating workspace: '{args.new_workspace}'", args.verbose, always=True)
        workspace_id = client.create_workspace(args.new_workspace)
        log(f"  Created workspace ID: {workspace_id}", args.verbose, always=True)
    else:
        parser.error("Specify either --workspace-id or --new-workspace.")
    time.sleep(15)

    
    folder_path = Path(args.input_folder)
    for file in folder_path.glob('*.yaml'):
        input_file = f"{args.input_folder}/{file.name}"
    
        # ------------------------------------------------------------------
        # Load input file
        # ------------------------------------------------------------------
        log(f"Loading input file: {input_file}", args.verbose, always=True)
        path, inputs = load_input_file(input_file)
        log(f"  path  : {path}", args.verbose)
        log(f"  inputs keys: {list(inputs.keys()) if isinstance(inputs, dict) else '<value>'}", args.verbose)

    

        # ------------------------------------------------------------------
        # Resolve studio ID
        # ------------------------------------------------------------------
        display_name = studio_name_from_filename(input_file)
        if display_name is None:
            parser.error(
                "Could not derive a Studio name from the filename. "
                "Expected pattern: Inputs_<DisplayName>.yaml. "
                "Use --studio-id to specify it explicitly."
            )
        log(f"\nAuto-detecting studio for displayName: '{display_name}'...",
            args.verbose, always=True)
        studio_id = client.find_studio_by_name(display_name)
        log(f"  Resolved studio ID: {studio_id}", args.verbose, always=True)

        

        # ------------------------------------------------------------------
        # Backup current inputs (enabled by default)
        # ------------------------------------------------------------------
        if args.backup:
            log(f"\nBacking up current studio inputs to '{args.backup_dir}'...",
                args.verbose, always=True)
            backup_path = save_backup(client, studio_id, path, input_file, args.backup_dir)
            if backup_path:
                log(f"  Backup saved: {backup_path}", args.verbose, always=True)
            else:
                log("  No existing inputs on mainline — skipping backup.", args.verbose, always=True)

        # ------------------------------------------------------------------
        # Set studio inputs
        # ------------------------------------------------------------------
        log(f"\nSetting studio inputs (studio: {studio_id})...", args.verbose, always=True)
            
        result = client.set_studio_inputs(
            workspace_id=workspace_id,
            studio_id=studio_id,
            inputs=inputs,
            path=path,
        )
        log(f"  Response: {json.dumps(result, indent=2)}", args.verbose)
        log("  Studio inputs updated successfully.", args.verbose, always=True)

    # ------------------------------------------------------------------
    # Optional: build workspace
    # ------------------------------------------------------------------
    if args.build:
        log(f"\nBuilding workspace {workspace_id}...", args.verbose, always=True)
        result = client.build_submit_workspace(workspace_id, 1)
        log(f"  Response: {json.dumps(result, indent=2)}", args.verbose)
        log("  Workspace built.", args.verbose, always=True)
        time.sleep(10)
    # ------------------------------------------------------------------
    # Optional: submit workspace
    # ------------------------------------------------------------------
    if args.submit:
        log(f"\nSubmitting workspace {workspace_id}...", args.verbose, always=True)
        result = client.build_submit_workspace(workspace_id, 3)
        log(f"  Response: {json.dumps(result, indent=2)}", args.verbose)
        log("  Workspace submitted.", args.verbose, always=True)

    log("\nDone.", always=True)


if __name__ == "__main__":
    main()
