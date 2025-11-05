from enum import Enum


class ReportTypeEnum(Enum):
    TOTAL_QUOTES = "total_quotes"
    LIVE_QUOTES = "live_quotes"
    LAPSED_QUOTES = "lapsed_quotes"
    QUOTE_COMPLETENESS = "quote_completeness"
    QUOTES_PET_TYPE = "quote_pet_type"
    QOUTES_CONVERTED = "quote_converted"
    Quote_RECEIVED_METHOD = "quote_received_mth"

class QuoteStatusEnum(str, Enum):
    LIVE = 'Live'
    LAPSED = 'Lapsed'
    ALL = 'All'