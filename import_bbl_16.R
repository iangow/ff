bbl_16 <- read.csv("bbl_16.csv", as.is=TRUE, row.names=NULL)

# Connect to my database
library(RPostgreSQL)
pg <- dbConnect(PostgreSQL())
dbWriteTable(pg, c("ff", "bbl_16"), bbl_16, overwrite=TRUE, row.names=FALSE)

sql <- paste0("
    COMMENT ON TABLE ff.bbl_16 IS
    'CREATED USING get_bbl_16.R ON ", Sys.time() , "';")
rs <- dbExecute(pg, paste(sql, collapse="\n"))

rs <- dbExecute(pg, "VACUUM ff.bbl_16")

dbExecute(pg, "ALTER TABLE ff.bbl_16 OWNER TO ff")
dbExecute(pg, "GRANT SELECT ON ff.bbl_16 TO ff_access")

rs <- dbDisconnect(pg)
