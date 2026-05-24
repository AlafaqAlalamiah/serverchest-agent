# ServerChest Agent

The ServerChest agent runs on your Odoo server and makes an outbound WebSocket connection to the [ServerChest](https://serverchest.com) dashboard, enabling remote monitoring, backup management, and server control — without opening any inbound ports.

## One-line install

Run this on your Odoo server as root. Get your API key from the ServerChest dashboard when you add a server.

```bash
curl -fsSL https://serverchest.com/install.sh | sudo bash -s -- --key=YOUR_API_KEY
```

The installer will:
- Auto-detect your Odoo installation (Python path, DB name, config file, service name)
- Install the `websockets` Python dependency
- Write `/etc/serverchest-agent.conf`
- Create and start `serverchest-agent.service` (systemd)

## Requirements

- Ubuntu / Debian (systemd)
- Python 3.9+
- Odoo 16 or 17
- Outbound HTTPS/WSS access to `serverchest.com`

## Configuration

The agent reads `/etc/serverchest-agent.conf`:

```ini
[agent]
relay_url     = wss://app.serverchest.com/ws/agent
api_key       = YOUR_API_KEY
backup_script = /opt/odoo17/backup_to_onedrive.sh
backup_log    = /var/log/odoo/backup.log
odoo_log      = /var/log/odoo/odoo17.log
rclone_config = /opt/odoo17/rclone.conf
odoo_conf     = /etc/odoo17.conf
odoo_bin      = /opt/odoo17/odoo17-venv/bin/python
odoo_src      = /opt/odoo17/odoo17/odoo-bin
db_name       = YOUR_DB_NAME
service_name  = odoo17
```

## Useful commands

```bash
# Check status
systemctl status serverchest-agent

# View live logs
journalctl -u serverchest-agent -f

# Restart
sudo systemctl restart serverchest-agent
```

## Security

- The agent runs as the Odoo system user (not root)
- Only outbound connections — no inbound ports required
- API key is stored in `/etc/serverchest-agent.conf` (permissions: 600)
- All communication is over WSS (WebSocket over TLS)
- Rotate your API key at any time from the ServerChest dashboard
