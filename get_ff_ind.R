########################################################################
# Small program to fetch and organize Fama-French industry data.
# The idea is to make a table that could be used for SQL merges.
########################################################################

library(DBI)
library(dplyr, warn.conflicts = FALSE)
library(tidyr)
library(readr)

# The URL for the data.
get_ff_ind <- function(num = 48) {
  t <- tempfile(fileext = ".zip") 
  
  base_url <- paste0("https://mba.tuck.dartmouth.edu/",
                     "pages/faculty/ken.french/ftp/Siccodes")
  url <- paste0(base_url, num, ".zip")
  
  download.file(url, t)
  
  ff_data <- 
    readr::read_fwf(t, 
                    col_positions = readr::fwf_widths(c(3, 7, NA),
                                                      c("ff_ind", 
                                                        "ff_ind_short_desc", 
                                                        "sic_range")),
                    col_types = "icc") %>%
    mutate(ff_ind_desc = if_else(!is.na(ff_ind_short_desc), 
                                 sic_range, NA_character_)) %>%
    tidyr::fill(ff_ind, ff_ind_short_desc, ff_ind_desc) %>%
    filter(grepl("^[0-9]", sic_range)) %>%
    tidyr::extract(sic_range, 
                   into = c("sic_min", "sic_max", "sic_desc"),
                   regex = "^([0-9]+)-([0-9]+)(.*)$",
                   convert = TRUE) %>%
    mutate(sic_desc = trimws(sic_desc))
  unlink(t)
  ff_data
}

write_ff_table <- function(i) {
  pg <- dbConnect(RPostgres::Postgres())
  
  dbExecute(pg, "SET search_path TO ff")
  
  get_ff_ind(i) %>% 
    dbWriteTable(pg, paste0("ind_", i), .,
                 overwrite=TRUE, row.names=FALSE)
  
  rs <- dbExecute(pg, paste0("VACUUM ind_", i))
  rs <- dbExecute(pg, paste0("CREATE INDEX ON ind_", i,
                              " (ff_ind)"))
  
  dbExecute(pg, paste0("ALTER TABLE ind_", i, " OWNER TO ff"))
  dbExecute(pg, paste0("GRANT SELECT ON ind_", i, " TO ff_access"))
  
  sql <- paste0("
    COMMENT ON TABLE ind_", i, " IS
    'CREATED USING get_ff_ind.R ON ", Sys.time() , "';")
  rs <- dbExecute(pg, paste(sql, collapse="\n"))
  rs <- dbDisconnect(pg)
}

# Get Fama-French industry data
lapply(c(12, 17, 48, 49L), write_ff_table)
