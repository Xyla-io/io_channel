import pandas as pd

from io_map import IOSingleSourceReporter, io_pruned_structure
from .context import IOChannelGranularity, IOChannelProperty, IOTimeGranularity, IOEntityGranularity,  IOTimeMetric, IOEntityMetric, IOEntityAttribute, IOReportOption
from datetime import datetime
from typing import Optional, Union, List, Dict, Type

class IOChannelSourceReporter(IOSingleSourceReporter):
  @classmethod
  def get_map_identifier(cls) -> str:
    return f'io_channel/{cls.__name__}'
  
  @property
  def io_to_api_map(self) -> Dict[IOChannelGranularity, Dict[IOChannelProperty, str]]:
    return io_pruned_structure({
      **{
        g: {
          e: self.io_to_api(
            raw_enum_value=e.value,
            raw_enum_prefix=g.value
          )
          for e in [
            *IOTimeMetric,
          ]
        }
        for g in IOTimeGranularity
      },
      **{
        g: {
          e: self.io_to_api(
            raw_enum_value=e.value,
            raw_enum_prefix=g.value
          )
          for e in [
            *IOEntityAttribute,
            *IOEntityMetric,
          ]
        }
        for g in IOEntityGranularity
      },
    }) or {}

  @property
  def filtered_io_entity_granularities(self) -> List[IOEntityGranularity]:
    return sorted(g for g in self.filtered_io_to_api_map() if isinstance(g, IOEntityGranularity))
    
  def get_from_options(self, option: IOReportOption) -> Optional[any]:
    return self.options[option.value] if option.value in self.options else None

  def get_from_filters(self, filter: str) -> Optional[any]:
    return self.filters[filter] if filter in self.filters else None

  def filtered_io_to_api_map(self, granularities: Optional[List[IOChannelGranularity]]=None, column_types: Optional[List[Type]]=None) -> Dict[IOChannelGranularity, Dict[IOChannelProperty, str]]:
    return io_pruned_structure({
      g: {
        e: c
        for e, c in m.items()
        if c is not None and (column_types is None or any(isinstance(e, column_type) for column_type in column_types)) and (f'{g.value}.{e.value}' in self.columns or e.value in self.columns)
      }
      for g, m in self.io_to_api_map.items()
      if granularities is None or g in granularities
    }) or {}

  def filtered_api_columns(self, granularity: Optional[IOChannelGranularity]=None, column_type: Optional[Type]=None) -> List[str]:
    return [
      c
      for g, m in self.filtered_io_to_api_map(
        granularities=[granularity] if granularity else None,
        column_types=[column_type] if column_type else None
      ).items()
      for e, c in m.items()
    ]

  def filtered_api_time_metrics(self, granularity: IOTimeGranularity) -> List[str]:
    return self.filtered_api_columns(
      granularity=granularity,
      column_type=IOTimeGranularity
    )

  def filtered_api_entity_metrics(self, granularity: IOEntityGranularity) -> List[str]:
    return self.filtered_api_columns(
      granularity=granularity,
      column_type=IOEntityMetric
    )

  def filtered_api_entity_attributes(self, granularity: IOEntityGranularity) -> List[str]:
    return self.filtered_api_columns(
      granularity=granularity,
      column_type=IOEntityAttribute
    )

  def io_entity_granularity_to_api(self, granularity: IOEntityGranularity) -> Optional[str]:
    return granularity.value

  def io_time_granularity_to_api(self, granularity: IOTimeGranularity) -> Optional[str]:
    return granularity.value

  def io_entity_attribute_to_api(self, attribute: IOEntityAttribute, granularity: Optional[IOEntityGranularity]=None) -> Optional[str]:
    return attribute.value

  def io_time_metric_to_api(self, metric: IOTimeMetric, granularity: Optional[IOTimeGranularity]=None) -> Optional[str]:
    return metric.value

  def io_entity_metric_to_api(self, metric: IOEntityMetric, granularity: Optional[IOEntityGranularity]=None) -> Optional[str]:
    return metric.value
  
  def io_to_api(self, raw_enum_value: str, raw_enum_prefix: Optional[str]=None) -> Optional[str]:
    if raw_enum_value in IOTimeGranularity.get_values():
      return self.io_time_granularity_to_api(
        granularity=IOTimeGranularity(raw_enum_value)
      )
    elif raw_enum_value in IOEntityGranularity.get_values():
      return self.io_entity_granularity_to_api(
        granularity=IOEntityGranularity(raw_enum_value)
      )
    elif raw_enum_value in IOTimeMetric.get_values():
      return self.io_time_metric_to_api(
        metric=IOTimeMetric(raw_enum_value),
        granularity=IOTimeGranularity(raw_enum_prefix) if raw_enum_prefix else None
      )
    elif raw_enum_value in IOEntityMetric.get_values():
      return self.io_entity_metric_to_api(
        metric=IOEntityMetric(raw_enum_value),
        granularity=IOEntityGranularity(raw_enum_prefix) if raw_enum_prefix else None
      )
    elif raw_enum_value in IOEntityAttribute.get_values():
      return self.io_entity_attribute_to_api(
         attribute=IOEntityAttribute(raw_enum_value),
         granularity=IOEntityGranularity(raw_enum_prefix) if raw_enum_prefix else None
      )

  def api_column_to_io(self, api_report: pd.DataFrame, api_column: str, granularity: IOChannelGranularity, property: IOChannelProperty) -> Optional[any]:
    if api_column not in api_report:
      return None
    else:
      return api_report[api_column]

  def api_report_to_io(self, api_report: pd.DataFrame, granularities: List[IOChannelGranularity]) -> pd.DataFrame:
    report = pd.DataFrame()
    filtered_map = self.filtered_io_to_api_map(granularities=granularities)
    for granularity, granularity_map in filtered_map.items():
      for property, api_column in granularity_map.items():
        column = f'{granularity.value}.{property.value}'
        report[column] = self.api_column_to_io(
          api_report=api_report,
          api_column=api_column,
          granularity=granularity,
          property=property
        )
    return report

  def api_ancestor_identifier_column_to_io(self, api_report: pd.DataFrame, io_report: pd.DataFrame, granularity: IOChannelGranularity, ancestor_granularity: IOChannelGranularity, api_column: str, api_ancestor_column: str, io_column: str, io_ancestor_column: str) -> Optional[any]:
    if isinstance(granularity, IOTimeGranularity):
      if io_column in io_report:
        if ancestor_granularity is IOTimeGranularity.daily:
          return pd.to_datetime(io_report[io_column]).apply(lambda d: datetime(d.year, d.month, d.day) if pd.notna(d) else None)
    column = self.api_column_to_io(
      api_report=api_report,
      api_column=api_ancestor_column,
      granularity=ancestor_granularity,
      property=ancestor_granularity.identifier_property
    )
    if column is not None:
      return column
    if io_ancestor_column in io_report:
      return io_report[io_ancestor_column]
    return None

  def fill_api_ancestor_identifiers_in_io(self, api_report: pd.DataFrame, io_report: pd.DataFrame, granularities: List[IOChannelGranularity]):
    for granularity in granularities:
      for ancestor_granularity in granularity.higher:
        api_column = self.io_to_api(granularity.identifier_property.value, granularity.value)
        api_ancestor_column = self.io_to_api(ancestor_granularity.identifier_property.value, ancestor_granularity.value)
        io_column = f'{granularity.value}.{granularity.identifier_property.value}'
        io_ancestor_column = f'{ancestor_granularity.value}.{granularity.identifier_property.value}'
        ancestor_identifier_column = self.api_ancestor_identifier_column_to_io(
          api_report=api_report,
          io_report=io_report,
          granularity=granularity,
          ancestor_granularity=ancestor_granularity,
          api_column=api_column,
          api_ancestor_column=api_ancestor_column,
          io_column=io_column,
          io_ancestor_column=io_ancestor_column
        )
        if io_ancestor_column in io_report and isinstance(ancestor_identifier_column, pd.Series):
          io_report[io_ancestor_column] = io_report[io_ancestor_column].update(ancestor_identifier_column)
        else:
          io_report[io_ancestor_column] = ancestor_identifier_column

  def finalized_io_report(self, io_report: pd.DataFrame) -> pd.DataFrame:
    columns = reversed(sorted(
      (g, p)
      for g, m in self.filtered_io_to_api_map().items()
      for p in m
    ))
    column_names = ['.'.join(e.value for e in c) for c in columns]
    finalized_report = self.finalize_report_columns(
      report=io_report,
      columns=column_names
    )
    finalized_report = self.finalize_report_rows(
      report=finalized_report,
      sort=column_names
    )
    return finalized_report