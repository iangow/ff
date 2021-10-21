########################################################################
# Small program to fetch and organize Fama-French factor data.
# The idea is to make a table that could be used for SQL merges.
########################################################################

library(dplyr, warn.conflicts = FALSE)
library(readr)
library(lubridate)

# The URL for the data.
ff.url.partial <- paste("http://mba.tuck.dartmouth.edu",
                        "pages/faculty/ken.french/ftp", sep="/")

# Function to remove leading and trailing spaces from a string
trim <- function(string) {
    ifelse(grepl("^\\s*$", string, perl=TRUE),"",
				gsub("^\\s*(.*?)\\s*$","\\1", string, perl=TRUE))
}

################################################################################
#             First download Fama-French three-factor data                     #
################################################################################

# Download the data and unzip it
ff.url <- paste(ff.url.partial, "F-F_Research_Data_Factors_daily_TXT.zip", sep="/")
f <- tempfile()
download.file(ff.url, f)

# Parse the data
ff_daily_factors <-
    read_fwf(f,
             col_positions = fwf_widths(c(8, 8, 8, 8, 10),
                                        c("date", "mktrf", "smb", "hml", "rf")),
             col_types = "c") %>%
  filter(grepl("^[0-9]+$", date)) %>%
  mutate_at(c("mktrf", "smb", "hml", "rf"), ~ round(as.double(.x)/100, 7)) %>%
  mutate(date = ymd(date))

################################################################################
#               Now download UMD (momentum) factor data                        #
################################################################################


# Download the data
ff.url <- paste(ff.url.partial, "F-F_Momentum_Factor_daily_TXT.zip", sep="/")
f <- tempfile()
download.file(ff.url, f)

# Parse the data
ff_mom_factor <-
  read_fwf(f,
           col_positions = fwf_widths(c(8, 8),
                                      c("date", "umd")),
           col_types = "c") %>%
  filter(grepl("^[0-9]+$", date)) %>%
  mutate_at("umd", ~ round(as.double(.x)/100, 7)) %>%
  mutate(date = ymd(date))
  
################################################################################
#                        Merge all the factor data                             #
################################################################################
ff_daily_factors <-
  ff_daily_factors %>%
  left_join(ff_mom_factor, by="date")

################################################################################
#                      Load the data into my database                          #
################################################################################
library(DBI)
pg <- dbConnect(RPostgres::Postgres())
rs <- dbExecute(pg, "SET search_path TO ff")
rs <- dbWriteTable(pg, "factors_daily_alt", ff_daily_factors,
                   overwrite=TRUE, row.names=FALSE)

sql <- paste0("
    COMMENT ON TABLE factors_daily_alt IS
    'CREATED USING get_ff_factors_daily_alt.R ON ", Sys.time() , "';")
rs <- dbExecute(pg, paste(sql, collapse="\n"))

rs <- dbExecute(pg, "ALTER TABLE factors_daily_alt OWNER TO ff")
rs <- dbExecute(pg, "GRANT SELECT ON factors_daily_alt TO ff_access")

rs <- dbExecute(pg, "VACUUM factors_daily_alt")
rs <- dbDisconnect(pg)

