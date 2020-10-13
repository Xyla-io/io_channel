from io_map import IOMap, io_pruned_structure
from typing import Dict, Optional
from enum import Enum

class IOEntityKey(Enum):
  channel = 'channel'
  id = 'id'
  granularity = 'granularity'
  parent_ids = 'parent_ids'
  state = 'state'
  update = 'update'

class IOEntity:
  entity: Dict[str, any]

  def __init__(self, entity: Dict[str, any]):
    self.entity = entity
  
  def io_property_to_api(self, io_property: str, io_context: Optional[str]=None) -> Optional[str]:
    return io_property

  def io_value_to_api(self, io_property: str, io_value: Optional[any], api_property: Optional[str]=None, io_context: Optional[str]=None) -> Optional[any]:
    return io_value

  def io_to_api(self, io_structure: Dict[str, any], io_context: Optional[str]=None) -> Dict[str, any]:
    return io_pruned_structure({
      self.io_property_to_api(k, io_context=io_context): self.io_value_to_api(k, v, io_context=io_context)
      for k, v in io_structure.items()
    })

class IOEntityCommitmentKey(Enum):
  api_url = 'api_url'
  api_request = 'api_request'
  api_response = 'api_response'
  dry_run = 'dry_run'

class IOEntityCommitter(IOMap):
  def run(self, entity: Dict[str, any], dry_run: bool=False) -> Dict[str, any]:
    pass