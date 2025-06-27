setwd("~/Documents/Dorey-upload-Bee-Library")

library(dplyr)
library(jsonlite)
library(stringr)

# ---- Load your data ----
data <- read.csv("05_cleaned_database_2024-02-15.csv", stringsAsFactors = FALSE)

# ---- Clean whitespace and malformed characters from all character fields ----
clean_text <- function(x) {
  if (is.character(x)) {
    x <- str_trim(x)
    x <- str_replace_all(x, "[^[:print:]]", "")
    x <- iconv(x, to = "UTF-8", sub = "")
  }
  return(x)
}
data <- data %>% mutate(across(where(is.character), clean_text))

data <- data %>% rename(institutioncode = institutionCode)

# ---- Replace all NA values with blank strings ----
data[is.na(data)] <- ""

# ---- Darwin Core standard terms (as of GBIF & TDWG) ----
darwin_core_terms <- c(
  # Occurrence
  "occurrenceID", "basisOfRecord", "catalogNumber", "recordNumber", "recordedBy",
  "individualCount", "sex", "lifeStage", "associatedTaxa", "associatedSequences",
  
  # Event
  "eventID", "eventDate", "eventTime", "year", "month", "day",
  "startDayOfYear", "endDayOfYear", "samplingProtocol", "samplingEffort","locationRemarks",
  
  # Location
  "country", "countryCode", "stateProvince", "county", "municipality", "locality",
  "decimalLatitude", "decimalLongitude", "coordinateUncertaintyInMeters", "verbatimLatitude", "verbatimLongitude",
  "verbatimElevation",
  
  # Taxon
  "scientificName", "scientificNameAuthorship", "taxonRank", "kingdom", "phylum",
  "class", "order", "family", "genus", "specificEpithet",
  "infraspecificEpithet",
  "identificationQualifier",
  
  # Identification
  "identifiedBy", "dateIdentified", "identificationReferences",
  "typeStatus",
  
  # Record-level
  "institutionCode", "collectionCode", "informationWithheld"
)

# ---- Identify non-DwC columns by name ----
non_dwc_cols <- setdiff(names(data), darwin_core_terms)

# ---- Construct dynamicProperties from non-DwC fields ----
data$dynamicProperties <- apply(data[, non_dwc_cols], 1, function(row) {
  row <- row[!is.na(row) & row != ""]
  if (length(row) == 0) return(NA_character_)
  jsonlite::toJSON(as.list(row), auto_unbox = TRUE)
})

# ---- Drop non-DwC fields EXCEPT database_id ----
cols_to_remove <- setdiff(non_dwc_cols, "database_id")
data <- data %>% select(-all_of(cols_to_remove))

# ---- Reorder columns to put database_id first ----
other_cols <- setdiff(names(data), "database_id")
data <- data[, c("database_id", other_cols)]

# ---- Write output for test ----
write.csv(data, "cleaned_data_dwc.csv", row.names = FALSE)

# ---- Split into chunks of 100,000 rows ----
chunk_size <- 100000
n <- ceiling(nrow(data) / chunk_size)

for (i in 1:n) {
  start_row <- (i - 1) * chunk_size + 1
  end_row <- min(i * chunk_size, nrow(data))
  chunk <- data[start_row:end_row, ]
  filename <- paste0("output/cleaned_data_chunk_", i, ".csv")
  write.csv(chunk, filename, row.names = FALSE)
}
