#!/usr/bin/env python3
"""Send a Specula run result to the person who triggered it, as a Slack DM.

Usage: slack_dm.py <github-login> <message-file> [<attachment>]

Env:
  SLACK_BOT_TOKEN      bot token (xoxb-...) with chat:write, files:write, im:write
  SPECULA_SLACK_USERS  JSON object mapping GitHub login -> Slack member ID

Exit code 2 means the login is not in SPECULA_SLACK_USERS (not allowed).
Never prints the token, the member ID or the message, because the job log is public.
"""
import json
import os
import sys
import urllib.parse
import urllib.request

API = "https://slack.com/api/"


def slack_user(login):
    users = json.loads(os.environ["SPECULA_SLACK_USERS"])
    return users.get(login)


def call(method, token, data, as_json=True):
    if as_json:
        body = json.dumps(data).encode()
        ctype = "application/json; charset=utf-8"
    else:
        body = urllib.parse.urlencode(data).encode()
        ctype = "application/x-www-form-urlencoded"
    req = urllib.request.Request(API + method, data=body, headers={
        "Authorization": "Bearer " + token, "Content-Type": ctype})
    with urllib.request.urlopen(req, timeout=60) as resp:
        out = json.load(resp)
    if not out.get("ok"):
        sys.exit(f"slack {method} failed: {out.get('error')}")
    return out


def main():
    if len(sys.argv) == 2:  # allowlist check only
        sys.exit(0 if slack_user(sys.argv[1]) else 2)

    login, message_file = sys.argv[1], sys.argv[2]
    attachment = sys.argv[3] if len(sys.argv) > 3 else None
    user = slack_user(login)
    if not user:
        sys.exit(2)
    token = os.environ["SLACK_BOT_TOKEN"]
    with open(message_file, encoding="utf-8") as f:
        text = f.read()[:3500]

    channel = call("conversations.open", token, {"users": user})["channel"]["id"]

    if not attachment or not os.path.exists(attachment):
        call("chat.postMessage", token, {"channel": channel, "text": text})
        print("sent message without attachment")
        return

    size = os.path.getsize(attachment)
    name = os.path.basename(attachment)
    up = call("files.getUploadURLExternal", token,
              {"filename": name, "length": size}, as_json=False)
    with open(attachment, "rb") as f:
        req = urllib.request.Request(up["upload_url"], data=f.read(),
                                     headers={"Content-Type": "application/octet-stream"})
        urllib.request.urlopen(req, timeout=300).read()
    call("files.completeUploadExternal", token, {
        "files": [{"id": up["file_id"], "title": name}],
        "channel_id": channel,
        "initial_comment": text,
    })
    print(f"sent message with {name} ({size} bytes)")


if __name__ == "__main__":
    main()
