import subprocess
import syslog

from psutil import Process
from pydantic import BaseModel
from slack_sdk import WebClient
from slack_sdk.models.attachments import Attachment


class User(BaseModel):
    username: str
    slack_user_id: str


class Config(BaseModel):
    users: list[User]
    slurm_process_identifiers: list[str]
    ignored_users: list[str]


def make_username2slackid(users: list[User]) -> dict[str, str]:
    username2slackid: dict[str, str] = {}
    for u in users:
        username2slackid[u.username] = u.slack_user_id
    return username2slackid


def notify_user(client: WebClient, slack_user_id: str, message: str):
    dm = client.conversations_open(users=slack_user_id)
    client.chat_postMessage(
        channel=dm["channel"]["id"],  # type: ignore
        text=message,
        attachments=[Attachment(color="#cc0000", text=message)],
    )


def should_process_be_killed(
    p: Process, slurm_process_identifiers: list[str], ignored_users: list[str]
) -> bool:
    _p = p
    while _p := _p.parent():
        if _p.name() in slurm_process_identifiers:
            return False
        if _p.username() in ignored_users:
            return False
    return True


if __name__ == "__main__":
    with open("config.json") as jsonfile:
        config = Config.model_validate_json(jsonfile.read())
    with open(".slack_api_token") as tokenfile:
        token = tokenfile.read().strip()

    # slack client
    client = WebClient(token=token)

    # transform users info
    username2slackid = make_username2slackid(users=config.users)

    # get all processes using any of those devices
    out = subprocess.check_output(
        ["lsof", "-t"] + ["/dev/nvidia0", "/dev/nvidia1"], text=True
    )
    # kill processes whose parent is not SLURM (slurmstepd)
    for pid in out.split():
        p = Process(pid=int(pid))
        if should_process_be_killed(
            p=p,
            slurm_process_identifiers=config.slurm_process_identifiers,
            ignored_users=config.ignored_users,
        ):
            syslog.syslog(
                f"Killing process PID {pid}: {p.name()} by ({p.username()}) - cannot use GPU outside of Slurm"
            )
            p.kill()
            notify_user(
                client=client,
                slack_user_id=username2slackid[p.username()],
                message=f"Process {pid}: {p.name()} was killed because it was consuming GPU outside Slurm job.",
            )
