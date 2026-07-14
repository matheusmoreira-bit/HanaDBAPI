"""Client helpers for the Be Compliance external API.

This module provides small helper functions to perform the two-step flow
from your n8n example:

- POST /auth/login with email/password to obtain access_token
- GET /third-party-analysis with Authorization: Bearer <token>

Usage examples:

>>> from becompliance import login, get_third_party_analysis
>>> token = login("servico.ti@anagaming.com.br", "Anasap123*", "8a179552-8dbb-4200-9ed0-def7ae8a5ccb")
>>> data = get_third_party_analysis(token, "8a179552-8dbb-4200-9ed0-def7ae8a5ccb")
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests


DEFAULT_HOST = "https://api.becompliance.com/ext/v1"


@dataclass
class BeComplianceClient:
    """Minimal client for Be Compliance API.

    Attributes:
        base_url: base URL (without trailing slash) pointing to the /ext/v1 path.
        tenant_id: the GUID part used in the URL in your example.
    """

    tenant_id: str
    base_url: str = DEFAULT_HOST

    def auth_url(self) -> str:
        return f"{self.base_url}/{self.tenant_id}/auth/login"

    def analysis_url(self) -> str:
        return f"{self.base_url}/{self.tenant_id}/third-party-analysis"

    def login(self, email: str, password: str, timeout: float = 10.0) -> Dict[str, Any]:
        """Authenticate and return the JSON response containing access_token.

        Raises requests.HTTPError on non-2xx responses.
        """
        payload = {"email": email, "password": password}
        resp = requests.post(self.auth_url(), json=payload, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    def get_third_party_analysis(self, access_token: str, timeout: float = 10.0) -> Any:
        """Call third-party-analysis endpoint using the provided bearer token.

        Returns parsed JSON. Raises requests.HTTPError on non-2xx responses.
        """
        headers = {"Authorization": f"Bearer {access_token}"}
        resp = requests.get(self.analysis_url(), headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp.json()


def login(email: str, password: str, tenant_id: str, base_url: Optional[str] = None) -> Dict[str, Any]:
    client = BeComplianceClient(tenant_id=tenant_id, base_url=base_url or DEFAULT_HOST)
    return client.login(email, password)


def get_third_party_analysis(token_response: Dict[str, Any], tenant_id: str, base_url: Optional[str] = None) -> Any:
    """Convenience wrapper that accepts either the token response dict (from login)
    or a raw access_token string.
    """
    if isinstance(token_response, dict):
        access_token = token_response.get("access_token")
    else:
        access_token = token_response  # type: ignore

    if not access_token:
        raise ValueError("access_token not found in token_response")

    client = BeComplianceClient(tenant_id=tenant_id, base_url=base_url or DEFAULT_HOST)
    return client.get_third_party_analysis(access_token)


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Quick test utility for Be Compliance API")
    parser.add_argument("--tenant", required=True, help="Tenant GUID part used in the URL")
    parser.add_argument("--email", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--analysis", action="store_true", help="Also fetch third-party-analysis after login")
    args = parser.parse_args()

    token = login(args.email, args.password, args.tenant)
    print("Login response:")
    print(json.dumps(token, indent=2, ensure_ascii=False))

    if args.analysis:
        data = get_third_party_analysis(token, args.tenant)
        print("Analysis response:")
        print(json.dumps(data, indent=2, ensure_ascii=False))
