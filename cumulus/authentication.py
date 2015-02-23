import logging
import pyrax
try:
    import swiftclient
except ImportError:
    swiftclient = None

from django.utils.functional import cached_property

from cumulus.settings import CUMULUS


class Auth(object):
    connection_kwargs = {}
    use_pyrax = CUMULUS["USE_PYRAX"]
    use_snet = CUMULUS["SERVICENET"]
    region = CUMULUS["REGION"]
    username = CUMULUS["USERNAME"]
    api_key = CUMULUS["API_KEY"]
    auth_url = CUMULUS["AUTH_URL"]
    auth_tenant_id = CUMULUS["AUTH_TENANT_ID"]
    auth_tenant_name = CUMULUS["AUTH_TENANT_NAME"]
    auth_version = CUMULUS["AUTH_VERSION"]
    pyrax_identity_type = CUMULUS["PYRAX_IDENTITY_TYPE"]
    container_uri = CUMULUS["CONTAINER_URI"]
    container_ssl_uri = CUMULUS["CONTAINER_SSL_URI"]

    def __init__(self, username=None, api_key=None, container=None, connection_kwargs=None):
        """
        Initializes the settings for the connection and container.
        """
        if username is not None:
            self.username = username
        if api_key is not None:
            self.api_key = api_key
        if container is not None:
            self.container_name = container
        if connection_kwargs is not None:
            self.connection_kwargs = connection_kwargs

        # connect
        if self.use_pyrax:
            self.pyrax = pyrax
            if self.pyrax_identity_type:
                self.pyrax.set_setting("identity_type", self.pyrax_identity_type)
            if self.auth_url:
                self.pyrax.set_setting("auth_endpoint", self.auth_url)
            if self.auth_tenant_id:
                self.pyrax.set_setting("tenant_id", self.auth_tenant_id)
            self.pyrax.set_setting("region", self.region)
            try:
                self.pyrax.set_credentials(self.username, self.api_key)
            except:
                logging.exception(
                    """Pyrax Connect Error in `django_cumulus.cumulus.authentication.Auth`::
                           self.pyrax.set_credentials(self.username, self.api_key)
                    """)
        # else:
        #     headers = {"X-Container-Read": ".r:*"}
        #     self._connection.post_container(self.container_name, headers=headers)

    def __getstate__(self):
        """
        Return a picklable representation of the storage.
        """
        return {
            "username": self.username,
            "api_key": self.api_key,
            "container_name": self.container_name,
            "use_snet": self.use_snet,
            "connection_kwargs": self.connection_kwargs
        }

    @cached_property
    def connection(self):
        if self.use_pyrax:
            public = not self.use_snet  # invert
            return pyrax.connect_to_cloudfiles(public=public)
        elif swiftclient:
            return swiftclient.Connection(
                authurl=self.auth_url,
                user=self.username,
                key=self.api_key,
                snet=self.use_snet,
                auth_version=self.auth_version,
                tenant_name=self.auth_tenant_name,
            )
        else:
            raise NotImplementedError("Cloud connection is not correctly configured.")

    @cached_property
    def container(self):
        """
        Gets or creates the container.
        """
        if self.use_pyrax:
            container = self.pyrax.cloudfiles.create_container(self.container_name)
            if container.cdn_ttl != self.ttl or not container.cdn_enabled:
                container.make_public(ttl=self.ttl)
        else:
            return None

    @cached_property
    def container_uri(self):
        if self.use_ssl and self.container_ssl_uri:
            container_uri = self.container_ssl_uri
        elif self.use_ssl:
            container_uri = self.container.cdn_ssl_uri
        else:
            container_uri = self.container.cdn_uri
        if CUMULUS["CNAMES"] and container_uri in CUMULUS["CNAMES"]:
            container_uri = CUMULUS["CNAMES"][container_uri]
        return container_uri
