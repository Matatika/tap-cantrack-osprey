"""Test Authentication."""

from __future__ import annotations

from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class TestAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for Test."""

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the AutomaticTestTap API.

        Returns:
            A dict with the request body
        """
        # TODO: Define the request body needed for the API.
        return {
            "UserName": self.config["username"],
            "Password": self.config["password"],
        }

    @classmethod
    def create_for_stream(cls, stream) -> TestAuthenticator:  # noqa: ANN001
        """Instantiate an authenticator for a specific Singer stream.

        Args:
            stream: The Singer stream instance.

        Returns:
            A new authenticator.
        """
        return cls(
            stream=stream, auth_endpoint="https://ospreyapi.cantrack.com/v1/api/token"
        )
