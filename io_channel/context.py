from enum import Enum
from io_map import IOOrderedEnum

class IOChannelProperty(IOOrderedEnum):
  pass

class IOChannelGranularity(IOOrderedEnum):
  @property
  def identifier_property(self) -> IOChannelProperty:
    raise NotImplementedError()

class IOTimeGranularity(IOChannelGranularity):
  hourly = 'hourly'
  daily = 'daily'

  @property
  def identifier_property(self) -> IOChannelProperty:
    return IOTimeMetric.time

  def __lt__(self, other):
    if other.__class__ is IOEntityGranularity:
      return False
    return super().__lt__(other)

class IOEntityGranularity(IOChannelGranularity):
  asset = 'asset'
  ad = 'ad'
  adgroup = 'adgroup'
  campaign = 'campaign'
  account = 'account'

  @property
  def identifier_property(self) -> IOChannelProperty:
    return IOEntityAttribute.id

  def __lt__(self, other):
    if other.__class__ is IOTimeGranularity:
      return True
    return super().__lt__(other)

class IOTimeMetric(IOChannelProperty):
  time = 'time'

class IOEntityAttribute(IOChannelProperty):
  id = 'id'
  name = 'name'
  type = 'type'
  status = 'status'
  daily_budget = 'daily_budget'
  goal_type = 'goal_type'
  goal = 'goal'
  bid_type = 'bid_type'
  bid = 'bid'
  currency = 'currency'
  timezone = 'timezone'

  def __lt__(self, other):
    if other.__class__ is IOEntityMetric:
      return False
    return super().__lt__(other)

class IOEntityMetric(IOChannelProperty):
  spend = 'spend'
  clicks = 'clicks'
  impressions = 'impressions'
  conversions = 'conversions'

  def __lt__(self, other):
    if other.__class__ is IOEntityAttribute:
      return True
    return super().__lt__(other)

class IOReportOption(Enum):
  time_granularity = 'time_granularity'