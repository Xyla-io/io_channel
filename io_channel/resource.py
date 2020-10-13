from io_map import IOMap
from data_layer import locator_factory, ResourceLocator, Decryptor, Cryptor
from typing import Optional, Dict, List

class IOSharedResourceMap(IOMap):
  url_key: str
  override_key: str
  encrypt_key: str
  encrypt_password_parameter: str
  schemes: List[str]

  def __init__(self, url_key: str='url', override_key: str='override', encrypt_key: str='encrypt', encrypt_password_parameter: str='password', schemes: List[str]=['s3', 'constant']):
    self.url_key = url_key
    self.override_key = override_key
    self.encrypt_key = encrypt_key
    self.encrypt_password_parameter = encrypt_password_parameter
    self.schemes = schemes

  def _check_scheme(self, locator: ResourceLocator):
    assert locator.url_parts.scheme in self.schemes, f'Unsupported shared resource scheme {locator.url_parts.scheme}'

  def run(self, resource: Dict[str, any]):
    if self.url_key not in resource:
      return resource
    shared_resource_locator = locator_factory(url=resource[self.url_key])
    self._check_scheme(locator=shared_resource_locator)

    encrypt = shared_resource_locator.get_locator_parameter('encrypt')
    if encrypt:
      assert self.encrypt_key in resource, f'Shared resouce missing property {self.encrypt_key} for encrypted URL'
      with Cryptor.local_registries():
        locator = locator_factory(url=resource[self.encrypt_key])
        self._check_scheme(locator=locator)
        private_key_bytes = locator.get()
        assert isinstance(private_key_bytes, bytes), f'Shared resource property {self.encrypt_key} must be a URL retrieving binary private key bytes'
        password = locator.get_locator_parameter(self.encrypt_password_parameter)
        decryptor = Decryptor(
          private_key=private_key_bytes,
          password=password.encode(),
          name=encrypt
        )
        Decryptor.register_decryptor(decryptor=decryptor)
        shared_resource = shared_resource_locator.get()
    else:
      shared_resource = shared_resource_locator.get()
    
    if self.override_key in resource:
      assert isinstance(resource[self.override_key], dict), f'Shared resource override property {self.override_key} must be of type dict'
      assert isinstance(shared_resource, dict), f'Shared resource must be of type dict to support overrrides in property {self.override_key}'
      shared_resource = {
        **shared_resource,
        **resource[self.override_key],
      }
    return shared_resource
    