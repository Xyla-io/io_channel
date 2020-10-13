import re

from enum import Enum
from io_map import IOMap, IOMapKey, IOMapOption
from typing import Optional, List, Dict, Union

class IOParser(IOMap):
  string: Optional[str]

  @property
  def labels(self) -> List[str]:
    return []

  def run(self, string: str) -> Dict[str, str]:
    return super().run(string=string)

class IOSwitchParser(IOParser):
  parser_identifier_key_map: Dict[str, any]
  parser_provider_key_map: Dict[str, any]
  parser_identifier: Optional[str]
  parser_key_map: Optional[Dict[str, any]]

  def __init__(self, parser_identifier_key_map: Dict[str, any], parser_provider_key_map: Dict[str, any]):
    self.parser_identifier_key_map = parser_identifier_key_map
    self.parser_provider_key_map = parser_provider_key_map

  @property
  def _key_maps(self) -> List[Dict[str, any]]:
    return [
      {
        IOMapKey.input.value: 'input.string',
        IOMapKey.output.value: 'run.parser_identifier',
        **self.parser_identifier_key_map,
      },
      {
        IOMapKey.input.value: 'run.parser_identifier',
        IOMapKey.output.value: 'run.parser_key_map',
        **self.parser_provider_key_map,
      },
      {
        IOMapKey.options.value: {
          IOMapOption.expand_at_run.value: 'run.parser_key_map',  
        },
        IOMapKey.input.value: 'input.string',
        IOMapKey.output.value: 'run.output'
      },
    ]

  @property
  def _run_keys(self) -> List[str]:
    return [
      'parser_identifier',
    ]

class IOSequenceParserKey(Enum):
  index = 'index'
  label = 'label'

class IOSequenceParser(IOParser):
  delimiter: str
  targets: Optional[List[Dict[str, any]]]

  def __init__(self, delimiter: str, targets: Optional[List[Dict[str, any]]]=None):
    self.delimiter = delimiter
    self.targets = [
      {
        **{k.value: None for k in IOSequenceParserKey},
        **t,
      }
      for t in targets
    ] if targets is not None else None
    if self.targets is not None:
      for target in self.targets:
        if target[IOSequenceParserKey.index.value] is not None:
          target[IOSequenceParserKey.index.value] = int(target[IOSequenceParserKey.index.value])

  @property
  def labels(self) -> List[str]:
    return list(filter(lambda v: v is not None, [
      t[IOSequenceParserKey.label.value] if IOSequenceParserKey.label.value in t else str(t[IOSequenceParserKey.index.value]) if IOSequenceParserKey.index.value in t else None
      for t in self.targets
    ])) if self.targets is not None else []

  def run(self, string: str) -> Dict[str, str]:
    components = string.split(self.delimiter)
    if self.targets is None:
      output = {str(i): v for i, v in enumerate(components)}
    else:
      output = {}
      component_count = len(components)
      for target in self.targets:
        index = target[IOSequenceParserKey.index.value]
        if index is None:
          continue
        label = target[IOSequenceParserKey.label.value]
        if label is None:
          label = str(index)
        if index >= component_count or -index > component_count:
          continue
        output[label] = components[index]
    return output

class IORegexParserKey(Enum):
  pattern = 'pattern'
  replacement = 'replacement'
  label = 'label'

class IORegexParser(IOParser):
  targets: List[Dict[str, any]]

  def __init__(self, targets: List[Dict[str, any]]=[]):
    self.targets = [
      {
        **{k.value: None for k in IORegexParserKey},
        **t,
      }
      for t in targets
    ]

  @property
  def labels(self) -> List[str]:
    return [
      t[IORegexParserKey.label.value] if IORegexParserKey.label.value in t else str(i)
      for i, t in enumerate(self.targets)
    ]

  def run(self, string: str) -> Dict[str, str]:
    output = {}
    for index, target in enumerate(self.targets):
      pattern = target[IORegexParserKey.pattern.value]
      if pattern is None:
        continue
      match = re.search(pattern, string)
      if not match:
        continue
      label = target[IORegexParserKey.label.value]
      if label is None:
       label = str(index)
      replacement = target[IORegexParserKey.replacement.value]
      output[label] = match.expand(replacement) if replacement is not None else match.group(0)
    return output