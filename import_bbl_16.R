library(readr)
library(DBI)
bbl_16 <- read_csv("bbl_16.csv", col_types="icii")

# Connect to my database
pg <- dbConnect(RPostgres::Postgres())

rs <- dbExecute(pg, "SET search_path TO ff")
dbWriteTable(pg, "bbl_16", bbl_16, overwrite=TRUE, row.names=FALSE)

sql <- paste0("
    COMMENT ON TABLE ff.bbl_16 IS
    'CREATED USING get_bbl_16.R ON ", Sys.time() , "';")
rs <- dbExecute(pg, paste(sql, collapse="\n"))

rs <- dbExecute(pg, "VACUUM bbl_16")

dbExecute(pg, "ALTER TABLE bbl_16 OWNER TO ff")
dbExecute(pg, "GRANT SELECT ON bbl_16 TO ff_access")

rs <- dbDisconnect(pg)
