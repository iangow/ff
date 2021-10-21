#!/usr/bin/env bash
psql -c "CREATE SCHEMA IF NOT EXISTS ff"

Rscript import_bbl_16.R
Rscript get_ff_ind.R
Rscript get_ff_ind_rets_monthly.R
Rscript get_ff_factors_daily.R
Rscript get_ff_factors_monthly.R
Rscript import_be_beme.R
Rscript get_ff_port_rets_monthly.R
Rscript get_ff_port_rets.R
