#!@PYTHON@ -tt

import atexit
import logging
import requests
import sys
import time
sys.path.append("@FENCEAGENTSLIBDIR@")

from fencing import *
from fencing import fail, fail_usage, run_delay, EC_LOGIN_DENIED, EC_STATUS, SyslogLibHandler

# As defined in https://docs.hetzner.cloud/#servers-get-all-servers
state = {
    "initializing": "unknown",
    "starting": "unknown",
    "running": "on",
    "stopping": "unknown",
    "off": "off",
    "deleting": "off",
    "rebuilding": "unknown",
    "migrating": "unknown",
    "unkown": "unknown"
}

logger = logging.getLogger("fence_hetzner")
logger.propagate = False
logger.setLevel(logging.INFO)
logger.addHandler(SyslogLibHandler())

def get_power_status(conn, options):
    logging.debug("Starting get status operation")
    server_id = options.get("--plug") # Plug ID from pcmk_host_map

    try:
        response = conn.get(f"https://api.hetzner.cloud/v1/servers/{server_id}")
        if options["--verbose-level"] > 1:
            logging.debug("API responded with a status code of %s", response.status_code)
            logging.debug("API responded with the following: %s", response.json())
        if response.status_code == 401:
            logging.error("API Error (unauthorized): Request was made with an invalid or unknown token")
            fail(EC_LOGIN_DENIED)
        elif response.status_code == 429: # https://docs.hetzner.cloud/#rate-limiting
            logging.warning("API Error (rate_limit_exceeded): Error when sending too many requests")
            logging.warning("Waiting a few seconds before trying again")
            time.sleep(3) # Give it a bit of time before trying again. Rate limit raises by 1 req/sec.
            return "unknown"
        server = response.json()['server']
        server_status = state[server['status']]
    except Exception as e:
        logging.error("Failed to get power status, with Exception: %s", e)
        fail(EC_STATUS)

    logging.debug("Status is %s", server_status)
    return server_status

def set_power_status(conn, options):
    logging.debug("Starting set status operation")
    server_id = options.get("--plug") # Plug ID from pcmk_host_map
    action = 'poweron' if options["--action"] == "on" else 'poweroff'

    try:
        response = conn.post(f"https://api.hetzner.cloud/v1/servers/{server_id}/actions/{action}")
        if options["--verbose-level"] > 1:
            logging.debug("API responded with a status code of %s", response.status_code)
            logging.debug("API responded with the following: %s", response.json())
        if response.status_code == 401:
            logging.error("API Error (unauthorized): Request was made with an invalid or unknown token")
            fail(EC_LOGIN_DENIED)
        elif response.status_code == 429: # https://docs.hetzner.cloud/#rate-limiting
            logging.warning("API Error (rate_limit_exceeded): Error when sending too many requests")
            logging.warning("Waiting a few seconds before trying again")
            time.sleep(3) # Give it a bit of time before trying again. Rate limit raises by 1 req/sec.
            set_power_status(conn, options) # There **should** be a better way to retry this?
        elif response.status_code == 423: # The item you are trying to access is locked (there is already an Action running)
            logging.warning("API Error (locked): The item you are trying to access is locked (there is already an Action running)")
            logging.warning("Waiting a few seconds before trying again")
            time.sleep(3) # Give it a bit of time before trying again.
            set_power_status(conn, options) # There **should** be a better way to retry this?
    except Exception as e:
        logging.error("Failed to set power status, with Exception: %s", e)
        fail(EC_STATUS)

def get_nodes_list(conn, options):
    logging.debug("Starting list/monitor operation")
    result = {}

    try:
        response = conn.get("https://api.hetzner.cloud/v1/servers")
        if options["--verbose-level"] > 1:
            logging.debug("API responded with a status code of %s", response.status_code)
            logging.debug("API responded with the following: %s", response.json())
        if response.status_code == 401:
            logging.error("API Error (unauthorized): Request was made with an invalid or unknown token.")
            fail(EC_LOGIN_DENIED)
        else:
            server_list = response.json()["servers"]
            number_of_servers = len(server_list)
            if number_of_servers > 0:
                logging.debug("Found %s servers", number_of_servers)
                for server in server_list:
                   result[str(server["id"])] = (server["name"], state[server['status']])
    except Exception as e:
        logging.error("Exception when calling Hetzner Cloud list: %s", e)

    return result

def define_new_opts():
    all_opt['api_token'] = {
        "getopt" : ":",
        "longopt" : "api-token",
        "help" : "--api-token=[apitoken]         Hetzner Cloud API token",
        "shortdesc" : "Hetzner Cloud API Key.",
        "required" : "1",
        "order" : 2
    }

# Main agent method
def main():
    device_opt = ["api_token", "no_password", "port"]

    atexit.register(atexit_handler)
    define_new_opts()

    all_opt["power_timeout"]["default"] = "40"

    options = check_input(device_opt, process_input(device_opt))

    docs = {}
    docs["shortdesc"] = "Fence agent for Hetzner Cloud"
    docs["longdesc"] = "fence_hetzner is an I/O Fencing agent for Hetzner's Cloud API to fence virtual machines."
    docs["vendorurl"] = "https://www.hetzner.com/cloud"
    show_docs(options, docs)

    run_delay(options)

    api_token = options.get("--api-token")

    # https://requests.readthedocs.io/en/latest/user/advanced/#session-objects
    conn = requests.Session()
    conn.headers.update({
        'Authorization': f'Bearer {api_token}',
        'Content-Type':'application/json',
        'User-Agent':'fence_hetzner',
    })

    result = fence_action(conn, options, set_power_status, get_power_status, get_nodes_list)
    sys.exit(result)

if __name__ == "__main__":
    main()
