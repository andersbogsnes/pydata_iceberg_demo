import httpx

from pydata.console import console
from pydata.exceptions import DremioAPIError


def get_token(client: httpx.Client, username: str = "dremio", password: str = "dremio123") -> str:
    return client.post("/apiv2/login",
                       json={"userName": username, "password": password}
                       ).json()["token"]


def create_nessie_source(client: httpx.Client) -> None:
    # Create Nessie source
    payload = {
        "entityType": "source",
        "name": "Nessie",
        "type": "NESSIE",
        "config": {
            "nessieEndpoint": "http://nessie:19120/api/v2",
            "nessieAuthType": "NONE",
            "credentialType": "ACCESS_KEY",
            "awsAccessKey": "minio",
            "awsAccessSecret": "minio1234",
            "awsRootPath": "/warehouse",
            "secure": "false",
            "propertyList": [
                {"name": "fs.s3a.endpoint", "value": "minio:9000"},
                {"name": "fs.s3a.path.style.access", "value": "true"},
                {"name": "dremio.s3.compat", "value": "true"}
            ]
        }
    }
    resp = client.post("/api/v3/catalog", json=payload)
    if resp.is_error:
        raise DremioAPIError(resp.json())
    return resp.json()


def create_minio_source(client: httpx.Client) -> None:
    payload = {
        "entityType": "source",
        "name": "Extract",
        "type": "S3",
        "config": {
            "accessKey": "minio",
            "accessSecret": "minio1234",
            "rootPath": "/datalake",
            "secure": "false",
            "compatibilityMode": "true",
            "propertyList": [
                {"name": "fs.s3a.endpoint", "value": "minio:9000"},
                {"name": "fs.s3a.path.style.access", "value": "true"},
            ]
        }
    }
    resp = client.post("/api/v3/catalog", json=payload)
    if resp.is_error:
        raise DremioAPIError(resp.json())
    return resp.json()


def create_dremio_sources(client: httpx.Client):
    token = get_token(client)
    client.headers.update({"Authorization": f"_dremio{token}"})
    console.print("[green]Creating Dremio sources[/green]")
    try:
        console.print("[purple]Setting up Nessie[/purple]")
        create_nessie_source(client)
    except DremioAPIError as e:
        console.print(f"[red]Failed to create Dremio sources: {e.args[0]['errorMessage']}[/red]")
    try:
        console.print("[purple]Setting up Minio[/purple]")
        create_minio_source(client)
    except DremioAPIError as e:
        console.print(f"[red]Failed to create Dremio sources: {e.args[0]['errorMessage']}[/red]")
