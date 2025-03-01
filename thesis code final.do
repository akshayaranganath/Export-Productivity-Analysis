clear
import delimited "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Data\Prowess\1\1.31\20231231_proditchs.dt"


*DESCRIPTIVE STATISTICS**************************************************************************************************************************************************************************************
tostring ca_finance1_year, replace
gen year = substr( ca_finance1_year, 1, 4)
gen month = substr( ca_finance1_year, 5, 2)
gen date = substr( ca_finance1_year, 7, 2)
gen length = length( ca_finance1_year)
destring year, replace
gen in_year = 1 if year < 2021 & year > 2009
replace in_year = 0 if in_year == .
gen export_ind = 0 if ca_export_earnings == "NA"
replace export_ind = 1 if ca_export_earnings != "NA"
tab in_year export_ind, m


***Ordered Merge file descriptive statistics
keep if export_ind == 1
br ca_export_earnings ca_export_earnings_pc_sales
gsort -ca_export_earnings
list ca_export_earnings NIC5digit nic_name co_code company_name if ca_export_earnings > 0 & _n <= 10, abbrev(20)

**Aim is to check TOP 20 Export Industries in India

*Step 1: Collapsing export earnings by industry (NIC 5 Digit Classification)
replace ca_export_earnings = "." if ca_export_earnings == "NA"
replace ca_industrial_sales = "." if ca_industrial_sales == "NA"
destring( ca_export_earnings), replace
destring( ca_industrial_sales), replace
collapse (sum) ca_export_earnings (first) product_name_mst nic_name, by(NIC5digit)
gsort - ca_export_earnings


**Aim is to check total no. of firms, exporting and non-exporting firms within each of the top 20 exporting industries
bysort NIC5digit (export_ind): generate firm_count = _N
collapse (count) total_firms=export_ind (sum) exporting_firms=export_ind, by(NIC5digit)
gen non_exporting_firms = total_firms - exporting_firms
gsort -exporting_firms
list NIC5digit total_firms exporting_firms non_exporting_firms if NIC5digit <=20
sort NIC5digit

*ASI**************************************************************************************************************************************************************************************

***Merging two blocks

cd "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Data\Annual Survey of Industries (ASI)\Block A, E" 

use blkA201617.dta"
ren dsl DSL
use blkA201718.dta"
ren a1 DSL
use blkA201819.dta"
ren A1 DSL
use blkA201920.dta" 
ren a1 DSL
use blkE201920.dta"
ren AE01 DSL
use blkE201819.dta"
ren AE01 DSL
use blkE201718.dta"
ren AE01 DSL

/*

Cleaning ASI 

*/

** 2009-10
gen average_wage_rate = E_Itm8 / E_Itm3
drop BLK DSL E_Itm1 E_Itm3 E_Itm4 E_Itm5 E_Itm6 E_Itm7 E_Itm8

**2010-11
rename Year YR
tostring YR , replace
replace YR =substr(YR, 3, 2)
gen average_wage_rate = 
drop BLK DSL Sno MManDay NMManDay TManDay AvgPersonWork MandaysPaid Wages


**2011-12
rename Year YR
tostring YR , replace
replace YR =substr(YR, 3, 2)
gen average_wage_rate = 
drop BLK DSL Sno MManDay NMManDay TManDay AvgPersonWork MandaysPaid Wages

**2012-13
rename Year YR
replace YR =substr(YR, 3, 2)
gen average_wage_rate = 

drop BLK DSL Sno MManDay NMManDay TManDay AvgPersonWork MandaysPaid Wages

**2013-14
rename Year YR
replace YR =substr(YR, 3, 2)
gen average_wage_rate = 

drop BLK DSL Sno MManDay NMManDay TManDay AvgPersonWork Wages MandaysPaid

**2014-15
rename Year YR
tostring YR , replace
replace YR =substr(YR, 3, 2)
gen average_wage_rate = 
drop BLK DSL SNO MManDay NMManDay TManDay AvgPersonWork MandaysPaid Wages

**2015-16
rename Year YR
tostring YR , replace
replace YR =substr(YR, 3, 2)
gen average_wage_rate = 
drop Block DSL S_No MandaysWorkedManuf MandaysWorkedNonManuf MandaysWorkedTotal NoofMandayspaid WagessalariesRs AveNumberPersonwork

**2016-17
gen average_wage_rate = 
drop block DSL Sno MandaysWorkedManuf MandaysWorkedNonManu MandaysWorkedTotal AvgNoofPersonsWorked NoofMandaysPaidfor wagesSalary

**2017-18
rename yr YR
gen average_wage_rate = 
drop blk AE01 E11 E13 E14 E15 E16 E17 E18

**201819
gen average_wage_rate = 
drop BLK AE01 E11 E13 E14 E15 E16 E17 E18

**2019-20
rename yr YR
gen average_wage_rate = 
drop BLK AE01 E11 E13 E14 E15 E16 E17 E18

********************************

*200910
ren yr YR
ren a5 NIC5digit
drop if missing(YR)
drop avg_wage_rate_mean
count if missing(avg_wage_rate)
drop if missing(avg_wage_rate)
bysort NIC5digit YR: egen avg_wage_rate_mean = mean(avg_wage_rate)
summarize avg_wage_rate avg_wage_rate_mean
replace YR = substr(YR, 3, 2)
tostring YR, replace
destring NIC5digit, replace

********************************
tostring(yr), replace
replace yr = substr(yr, 3, 2)
ren yr YR
keep YR total_emoluments avg_wage_rate avg_wage_rate_mean

cd "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Data\Annual Survey of Industries (ASI)\Block A, E" 

use blka200910.dta
generate total_emoluments = E_Itm10 + E_Itm11 + E_Itm12
summarize total_emoluments
duplicates report total_emoluments 
duplicates list total_emoluments A_Itm5 A_Itm9 A_Itm8 A_Itm7
duplicates drop total_emoluments, force
merge 1:m DSL using blke200910.dta
gen avg_wage_rate = total_emoluments / E_Itm5
drop if missing(avg_wage_rate)
drop avg_wage_rate_mean
bysort A_Itm5 YR: egen avg_wage_rate_mean = mean(avg_wage_rate)
summarize avg_wage_rate
summarize avg_wage_rate_mean
count if missing(avg_wage_rate_mean)
drop BLK A_Itm2 A_Itm3 A_Itm4 A_Itm8 A_Itm9 A_Itm10 A_Itm11 A_Itm12 E_Itm13a E_Itm13b E_Itm13c E_Itm14 J_Itm13 WGT E_Itm4 E_Itm3 E_Itm6 E_Itm7 A_Itm7


use blka201011.dta
generate total_emoluments = Bonus + ProvidentFund + Welfare
summarize total_emoluments
duplicates report total_emoluments 
duplicates list total_emoluments NIC5digit
duplicates drop total_emoluments, force
merge 1:m DSL using blke201011.dta
gen avg_wage_rate = total_emoluments / TManDay
duplicates report avg_wage_rate
duplicates drop avg_wage_rate, force
bysort NIC5digit Year: egen avg_wage_rate_mean = mean(avg_wage_rate)
summarize avg_wage_rate
summarize avg_wage_rate_mean
count if missing(avg_wage_rate_mean)

use blka201112.dta
generate total_emoluments = Bonus + ProvidentFund + Welfare
summarize total_emoluments
duplicates report total_emoluments 
duplicates list total_emoluments NIC5digit
duplicates drop total_emoluments, force
merge 1:m DSL using blke201112.dta
gen avg_wage_rate = total_emoluments / TManDay
duplicates report avg_wage_rate
bysort NIC5digit: egen avg_wage_rate_mean = mean(avg_wage_rate)
summarize avg_wage_rate
summarize avg_wage_rate_mean
count if missing(avg_wage_rate)
drop if missing(avg_wage_rate)

use blka201213.dta
preserve
generate total_emoluments = Bonus + ProvidentFund + Welfare
summarize total_emoluments
duplicates report total_emoluments 
duplicates list total_emoluments NIC5digit
duplicates drop total_emoluments, force
merge 1:m DSL using blke201213.dta
gen avg_wage_rate = total_emoluments / TManDay
duplicates report avg_wage_rate
bysort NIC5digit: egen avg_wage_rate_mean = mean(avg_wage_rate)
summarize avg_wage_rate
summarize avg_wage_rate_mean
count if missing(avg_wage_rate)
drop if missing(avg_wage_rate)
save 
restore 


use blka201314.dta
preserve
generate total_emoluments = Bonus + ProvidentFund + Welfare
summarize total_emoluments
duplicates report total_emoluments 
duplicates list total_emoluments NIC5digit
duplicates drop total_emoluments, force
merge 1:m DSL using blke201314.dta
gen avg_wage_rate = total_emoluments / TManDay
duplicates report avg_wage_rate
bysort NIC5digit: egen avg_wage_rate_mean = mean(avg_wage_rate)
summarize avg_wage_rate
summarize avg_wage_rate_mean
count if missing(avg_wage_rate)
drop if missing(avg_wage_rate)

use blka201415.dta
generate total_emoluments = Bonus + ProvidentFund + Welfare
summarize total_emoluments
duplicates report total_emoluments 
duplicates list total_emoluments INC5digit
duplicates drop total_emoluments, force
merge 1:m DSL using blke201415.dta, force
gen avg_wage_rate = total_emoluments / TManDay
duplicates report avg_wage_rate
bysort INC5digit: egen avg_wage_rate_mean = mean(avg_wage_rate)
summarize avg_wage_rate
summarize avg_wage_rate_mean
count if missing(avg_wage_rate)
drop if missing(avg_wage_rate)

use blka201516.dta
generate total_emoluments = BONUS + PROVIDENT_FUND + WELFARE
summarize total_emoluments
duplicates report total_emoluments 
duplicates list total_emoluments IND_CD
duplicates drop total_emoluments, force
merge 1:m DSL using blke201516.dta, force
tostring(DSL), replace
gen avg_wage_rate = total_emoluments / MandaysWorkedTotal
duplicates report avg_wage_rate
bysort IND_CD: egen avg_wage_rate_mean = mean(avg_wage_rate)
summarize avg_wage_rate
summarize avg_wage_rate_mean
count if missing(avg_wage_rate)
drop if missing(avg_wage_rate)

use blka201617.dta
generate total_emoluments = bonus + pf + welfare
summarize total_emoluments
duplicates report total_emoluments 
duplicates list total_emoluments ind_cd_return
duplicates drop total_emoluments, force
merge 1:m DSL using blke201617.dta, force
tostring(DSL), replace
gen avg_wage_rate = total_emoluments / MandaysWorkedTotal
duplicates report avg_wage_rate
bysort ind_cd_return: egen avg_wage_rate_mean = mean(avg_wage_rate)
summarize avg_wage_rate
summarize avg_wage_rate_mean
count if missing(avg_wage_rate)
drop if missing(avg_wage_rate)

use blka201718.dta
generate total_emoluments = bonus + pf + welfare
summarize total_emoluments
duplicates report total_emoluments 
duplicates list total_emoluments a5
duplicates drop total_emoluments, force
merge 1:m DSL using blke201718.dta, force
*tostring(DSL), replace
gen avg_wage_rate = total_emoluments / E15
duplicates report avg_wage_rate
bysort a5: egen avg_wage_rate_mean = mean(avg_wage_rate)
summarize avg_wage_rate
summarize avg_wage_rate_mean
count if missing(avg_wage_rate)
drop if missing(avg_wage_rate)

use blka201819.dta
generate total_emoluments = BONUS + PF + WELFARE
summarize total_emoluments
duplicates report total_emoluments 
duplicates list total_emoluments A5
duplicates drop total_emoluments, force
merge 1:m DSL using blke201819.dta, force
*tostring(DSL), replace
gen avg_wage_rate = total_emoluments / E15
duplicates report avg_wage_rate
bysort A5: egen avg_wage_rate_mean = mean(avg_wage_rate)
summarize avg_wage_rate
summarize avg_wage_rate_mean
count if missing(avg_wage_rate)
drop if missing(avg_wage_rate)

use blka201920.dta
generate total_emoluments = bonus + pf + welfare
summarize total_emoluments
duplicates report total_emoluments 
duplicates list total_emoluments a5
duplicates drop total_emoluments, force
merge 1:m DSL using blke201920.dta, force
*tostring(DSL), replace
gen avg_wage_rate = total_emoluments / E15
duplicates report avg_wage_rate
bysort a5: egen avg_wage_rate_mean = mean(avg_wage_rate)
summarize avg_wage_rate
summarize avg_wage_rate_mean
count if missing(avg_wage_rate)
drop if missing(avg_wage_rate)

*Merging ASI Prowess for Avg Wage Rate**************************************************************************************************************************************************************************************
gen YR = substr(ca_finance1_year, 3, 2)

use "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Data\Annual Survey of Industries (ASI)\Block E 2010-2022\blke201011.dta"
duplicates report DSL YR
duplicates list DSL YR
list Wages MandaysPaid NMManDay MManDay if DSL ==  44044
list Wages MandaysPaid NMManDay MManDay if DSL ==  44044 & Year == 10

ren ca_company_name company_name
ren ca_finance1_cocode co_code

***Merging WPI 2005-06 with Prowess ASI Merged
use "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Yearly WPI India Base 2005 till 2016.dta"
reshape long IN, i(COMM_CODE) j(year)
tostring(year), replace
gen YR = substr(year, 3, 2)
order YR 



/*
    Result                           # of obs.
    -----------------------------------------
    not matched                             7
        from master                         0  (_merge_cap_def==1)
        from using                          7  (_merge_cap_def==2)

    matched                            45,618  (_merge_cap_def==3)
    -----------------------------------------
*/

count if missing(ca_gross_fixed_assets)
replace ca_gross_fixed_assets = "." if ca_gross_fixed_assets == "NA"
count if missing( ca_gross_fixed_assets)
destring ca_gross_fixed_assets, replace
gen capital_inputs = ca_gross_fixed_assets / GCF_million_usd

*******WPI 2011-12
ren A comm_name
ren B comm_code
ren C comm_wt
rename D yr2010
rename E yr2011
rename F yr2012
rename G yr2013
rename H yr2014
rename I yr2015
rename J yr2016
rename K yr2017
rename L yr2018
rename M yr2019
rename N yr2020
drop in 1/1
reshape long yr, i(comm_name comm_code comm_wt) j(year)


*********Energy Price Index 2011-12
ren A year
ren B overall_index
ren C coal
ren D crude_oil
ren E natural_gas
ren F petroleum
ren G fertilizers
ren H steel
ren I cement
ren J electricity
drop in 1/163
gen YR = substr(year, 6, 2)
save "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\2011-12 Energy Price Index.dta", replace

count if missing(ca_power_and_fuel_exp)
replace ca_power_and_fuel_exp = "." if ca_power_and_fuel_exp == "NA"
count if missing( ca_power_and_fuel_exp)
destring(ca_power_and_fuel_exp), replace
destring(electricity), replace
gen energy_inputs = ca_power_and_fuel_exp / electricity
br energy_inputs

****Merging energy concumption
ren totener_cocode co_code
replace energy_cons_qty = ."" if energy_cons_qty == "NA" 
replace energy_cons_value = "." if energy_cons_value == "NA" 
destring energy_cons_qty, replace
destring energy_cons_value, replace
collapse (sum) energy_cons_qty energy_cons_value (first) company_name energy_name totener_date product_name_mst energy_cons_unit energy_cons_rate_per_unit energy_cons_rate_unit, by(co_code YR) 

merge m:1 co_code YR using "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\energy cons trial.dta", gen(_merge_energy_cons)

/*
    Result                           # of obs.
    -----------------------------------------
    not matched                       111,476
        from master                    36,461  (_merge_energy_cons==1)
        from using                     75,015  (_merge_energy_cons==2)

    matched                             4,655  (_merge_energy_cons==3)
    -----------------------------------------
*/

*****Merging Raw Materials

keep if ann_rep_months == 12
br company_name rawmat_consump_val rawmat_date
tab rawmat_consump_val
ren rawmater_cocode co_code
describe rawmat_date
tostring( rawmat_date), replace
gen YR = substr(rawmat_date, 3, 2)
tab YR, m
destring( rawmat_consump_val), replace
replace rawmat_consump_val = "." if rawmat_consump_val == "NA"
destring( rawmat_consump_val), replace
collapse (sum) rawmat_consump_val (first) company_name rawmat_date ann_rep_months rawmat_name rawmat_code product_name_mst rawmat_consump_qty clubflag_rawmat_consump_qty rawmat_consump_unit clubflag_rawmat_consump_val , by( co_code YR)
save "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Raw Material Consupmtion Prowess.dta"
merge m:1 YR co_code using "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Raw Material Consupmtion Prowess.dta", gen(_merge_rawmat)
br YR if _merge_rawmat == 1
br YR rawmat_consump_val if _merge_rawmat == 1
br YR rawmat_consump_val if _merge_rawmat == 3
save "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Version 7.dta"



*Cleaning Energy Consumption File
*Kept only relevant years, collapsed energy info so that 1 firm has 1 value for all types of ebergy consumed (sum of it). This is monetary value and NOT quantity consumed therefore it is ok to merge
ren totener_cocode co_code
gen year = substr( totener_date, 1, 4)
tab year
destring(year), replace
drop if year < 2010 | year > 2020
tab year, m
replace energy_cons_qty = "." if energy_cons_qty == "NA" 
replace energy_cons_value = "." if energy_cons_value == "NA" 
destring energy_cons_qty, replace
destring energy_cons_value, replace
drop if missing(energy_cons_value)
collapse (sum) energy_cons_value (first)  energy_cons_qty company_name energy_name totener_date product_name_mst energy_cons_unit energy_cons_rate_per_unit energy_cons_rate_unit, by(co_code YR) 

save "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Ordered Energy Consumption Prowess.dta"


/***********

Ordered Merge RESTART

*/

*Keeping only relevant years i.e. 2010-2020 and dropping the rest
use "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Prowess Annual and Financial Statements.dta"
preserve
drop if year < 2010 | year > 2020
save "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Ordered Merge Annual Financial Statements.dta"

*After cleaning both files, mrging Prowess annual finncial statements with prowess identity and company information
*Successfully merged with 40,105 obs left with export indicator, year and YR variables

use "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Ordered Merge Annual Financial Statements.dta"
merge m:1 co_code using "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Identity and Company Address.dta"

*ASI has industry wise average wage rate in USD million. labor inputs is salaries / avg wage rate inds wise USD Million 


***********Capital Deflator Cleaning
drop in 1/3
br
ren v1 Year
ren v2 Population_lakhs
ren v3 GVA_Basic_Prices
ren v4 Net_taxes_on_products
ren v5 Gross_Domestic_Product
ren v6 Consumption_of_Fixed_Capital
ren v7 Net_Domestic_Product
ren v8 Primary_inc_receivable_net
label variable Primary_inc_receivable_net "Primary income receivable from ROW (net)"
ren v9 Gross_National_Income
ren v10 Net_National_Income
ren v11 Other_current_transfers_net
label variable Other_current_transfers_net "Other current transfers (net) from ROW"
ren v12 Gross_National_Disposable_Income
ren v13 Net_National_Disposable_Income
ren v14 Gross_Saving
ren v15 Net_Savings
ren v16 Gross_Capital_Formation
ren v17 Net_Capital_Formation
ren v18 Per_Capita_GDP
label variable Per_Capita_GDP "Per Capita GDP (₹)"
ren v19 Per_Capita_GNI
label variable Per_Capita_GNI "Per Capita GNI (₹)"
ren v20 Per_Capita_NNI
label variable Per_Capita_NNI "Per Capita NNI (₹)"
ren v21 Per_Capita_GNDI
label variable Per_Capita_GNDI "Per Capita GNDI (₹)"
ren v22 Per_Capita_PFCE
label variable Per_Capita_PFCE "Per Capita PFCE (₹)"
ren v23 GNP_Basic_Prices
ren v24 NNP_Basic_Prices
ren v25 NVA_Basic_Prices
ren v26 per_capita_GNI
label variable per_capita_GNI "per capita GNI at factor cost"
drop in 1/2
drop in 21/28
save "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\2011-12 Capital Deflator.dta"

drop in 1/2
destring(Gross_Capital_Formation), replace
gen Gross_Capital_Formation_clean = subinstr(Gross_Capital_Formation, ",", "", .)
destring Gross_Capital_Formation_clean, replace
list Gross_Capital_Formation_clean
gen GCF_million_usd = (Gross_Capital_Formation_clean * 10000000) / 53.383
gen YR = substr(Year, 3, 2)

****Merging Capital Deflator
merge m:1 YR using "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\2011-12 Capital Deflator Million US.dta", generate(_merge_cap_def)

/*


    Result                           # of obs.
    -----------------------------------------
    not matched                             7
        from master                         0  (_merge_cap_def==1)
        from using                          7  (_merge_cap_def==2)

    matched                            13,567  (_merge_cap_def==3)
    -----------------------------------------

*/

describe Gross_Capital_Formation_clean ca_gross_fixed_assets
destring(ca_gross_fixed_assets), replace
replace ca_gross_fixed_assets = "." if ca_gross_fixed_assets == "NA"
destring(ca_gross_fixed_assets), replace
drop if missing( ca_gross_fixed_assets)
gen capital_inputs = ca_gross_fixed_assets / Gross_Capital_Formation_clean
br capital_inputs co_code company_name YR
order co_code company_name YR ca_sales capital_inputs labor_inputs ca_power_and_fuel_exp ca_rawmat_exp ca_gross_fixed_assets avg_wage_rate NIC5digit nic_name GCF_million_usd Gross_Capital_Formation_clean

*Capital Deflator merged USD Million


/***********

WPI to Prowess Mapping for top 20 export industries

*/


import excel "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\WPI Industry Mapping.xlsx", sheet("Merge Sheet")
ren B NIC5digit
ren C WPI_name
ren D WPI_comm_code
ren E NIC_name
drop A
drop in 1/1
save "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\WPI Prowess Mapped.dta"

local codes 'x' 1202000010 1202000007 1301040001 1301040002 1301040003 1301040004 1301040005 1301040006 1301040007 1301040008 1301040009 1301040010 1301040011 1301040000 1317000000 1317010000 1317010001 1317010002 1317010003 1317010004 1317010005 1317010006 1317010007 1317010008 1317010009 1317010010 1317010011 1317010012 1317010013 1317010014 1317010015 1317020000 1317020001 1317020002 1317020003 1317020004 1317030000 1317030001 1317040000 1317040001 1317040002 1317040003 1317040004 1317040005 1317040006 1317040007 1317040008 1317040009 1317040010 1317050000 1317050001 1317050002 1317050003 1317050004 1317050005 1317050006 1317060000 1317060001 1317060002 1317060003 1317060004 1317060005 1317060006 1317060007 1317060008 1317060009 1317070000 1317070001 1317070002 1317070003 1322010000 1322010001 1322010002 1304020001 1304020004 1314030000 1314030001 1314030002 1314030003 1314040000 1314040001 1314040002 1314040003 1314040004 1314050000 1314050001 1314050002 1314050003 1314050004 1310000000 1310010000 1310010001 1310010002 1310010003 1310010004 1310010005 1310010006 1310010007 1310010008 1310010009 1310010010 1310010011 1310010012 1310010013 1310010014 1310010015 1310010016 1310010017 1310010018 1310010019 1310010020 1310010021 1310010022 1310010023 1310010024 1310010025 1310010026 1310010027 1310020000 1310020001 1310020002 1310020003 1310020004 1310020005 1310020006 1310020007 1310020008 1310020009 1310030000 1310030001 1310030002 1310030003 1310030004 1310030005 1310030006 1310040000 1310040001 1310040002 1310040003 1310050000 1310050001 1310050002 1310050003 1310050004 1310060000 1310060001 1310060002 1310060003 1310060004 1310060005 1310060006 1310060007 1310060008 1310060009 1310060010 1310060011 1304010001 1304020001 1304020004 1304030001 1304030002 1304040004 1304040005 1305010001 1305010002 1305020004 1314090003 1314090004 1314090007 1314090008 1314090009 1314090010 1314090012 1314100004 1315060004 1317040005 1317040006 1311000000 1311010000 1311010001 1311010002 1311010003 1311010004 1311010005 1311010006 1311010007 1311010008 1311010009 1311010010 1311010011 1311010012 1311010013 1311010014 1311010015 1311010016 1311010017 1311010018 1311010019 1311010020 1311010021 1311010022 1311010023 1318090001 1314080001 1314080002 1315010001 1312010007 1312010010 1310010003 1101040002 1101040003 1301020000 1301020001 1301020002 1310030000 1310030001 1310030002 1310030003 1310030004 1310030005 1310030006 1316030000 1316030001 1316030002

gen keep_code = 0
destring( comm_code), replace
replace keep_code = 1 if comm_code == 1202000010 | comm_code == 1202000007 | comm_code == 1301040001 |comm_code == 1301040002 | comm_code == 1301040003 | comm_code == 1301040004 | comm_code == 1301040005 | comm_code == 1301040006 | comm_code == 1301040007 | comm_code == 1301040008 | comm_code == 1301040009 | comm_code == 1301040010 | comm_code == 1301040011 | comm_code == 1301040000 | comm_code == 1317000000 | comm_code == 1317010000 |comm_code == 1317010001 | comm_code == 1317010002| comm_code == 1317010003 | comm_code == 1317010004
replace keep_code = 1 if comm_code == 1317010005 | comm_code == 1317010006 | comm_code == 1317010007 | comm_code == 1317010008 | comm_code == 1317010009 | comm_code == 1317010010 | comm_code == 1317010011 | comm_code == 1317010012 | comm_code == 1317010013 | comm_code == 1317010014 | comm_code == 1317010015 | comm_code == 1317020000 | comm_code == 1317020001 | comm_code == 1317020002 | comm_code == 1317020003 | comm_code == 1317020004 | comm_code == 1317030000 | comm_code == 1317030001 | comm_code == 1317040000 | comm_code == 1317040001 | comm_code == 1317040002 | comm_code == 1317040003
replace keep_code = 1 if comm_code == 1317040004 | comm_code == 1317040005 | comm_code == 1317040006 | comm_code == 1317040007 | comm_code == 1317040008 | comm_code == 1317040009 | comm_code == 1317040010 | comm_code == 1317050000 | comm_code == 1317050001 | comm_code == 1317050002 | comm_code == 1317050003 | comm_code == 1317050004 | comm_code == 1317050005 | comm_code == 1317050006 | comm_code == 1317060000 | comm_code == 1317060001 | comm_code == 1317060002 | comm_code == 1317060003 | comm_code == 1317060004 | comm_code == 1317060005 | comm_code == 1317060006 | comm_code == 1317060007
replace keep_code = 1 if comm_code == 1317060008 | comm_code == 1317060009 | comm_code == 1317070000 | comm_code == 1317070001 | comm_code == 1317070002 | comm_code == 1317070003 | comm_code == 1322010000 | comm_code == 1322010001 | comm_code == 1322010002 | comm_code == 1304020001 | comm_code == 1304020004 | comm_code == 1314030000 | comm_code == 1314030001 | comm_code == 1314030002 | comm_code == 1314030003 | comm_code == 1314040000 | comm_code == 1314040001 | comm_code == 1314040002 | comm_code == 1314040003 | comm_code == 1314040004 | comm_code == 1314050000 | comm_code == 1314050001
replace keep_code = 1 if comm_code == 1314050002 | comm_code == 1314050003 | comm_code == 1314050004 | comm_code == 1310000000 | comm_code == 1310010000 | comm_code == 1310010001 | comm_code == 1310010002 | comm_code == 1310010003 | comm_code == 1310010004 | comm_code == 1310010005 | comm_code == 1310010006 | comm_code == 1310010007 | comm_code == 1310010008 | comm_code == 1310010009 | comm_code == 1310010010 | comm_code == 1310010011 | comm_code == 1310010012 | comm_code == 1310010013 | comm_code == 1310010014 | comm_code == 1310010015 | comm_code == 1310010016 | comm_code == 1310010017
tab keep_code
replace keep_code = 1 if comm_code == 1310010018 | comm_code == 1310010019 | comm_code == 1310010020 | comm_code == 1310010021 | comm_code == 1310010022 | comm_code == 1310010023 | comm_code == 1310010024 | comm_code == 1310010025 | comm_code == 1310010026 | comm_code == 1310010027 | comm_code == 1310020000 | comm_code == 1310020001 | comm_code == 1310020002 | comm_code == 1310020003 | comm_code == 1310020004 | comm_code == 1310020005 | comm_code == 1310020006 | comm_code == 1310020007 | comm_code == 1310020008 | comm_code == 1310020009 | comm_code == 1310030000 | comm_code == 1310030001
replace keep_code = 1 if comm_code == 1310030002 | comm_code == 1310030003 | comm_code == 1310030004 | comm_code == 1310030005 | comm_code == 1310030006 | comm_code == 1310040000 | comm_code == 1310040001 | comm_code == 1310040002 | comm_code == 1310040003 | comm_code == 1310050000 | comm_code == 1310050001 | comm_code == 1310050002 | comm_code == 1310050003 | comm_code == 1310050004 | comm_code == 1310060000 | comm_code == 1310060001 | comm_code == 1310060002 | comm_code == 1310060003 | comm_code == 1310060004 | comm_code == 1310060005 | comm_code == 1310060006 | comm_code == 1310060007
tab keep_code
replace keep_code = 1 if comm_code == 1310060008 | comm_code == 1310060009 | comm_code == 1310060010 | comm_code == 1310060011 | comm_code == 1304010001 | comm_code == 1304020001 | comm_code == 1304020004 | comm_code == 1304030001 | comm_code == 1304030002 | comm_code == 1304040004 | comm_code == 1304040005 | comm_code == 1305010001 | comm_code == 1305010002 | comm_code == 1305020004 | comm_code == 1314090003 | comm_code == 1314090004 | comm_code == 1314090007 | comm_code == 1314090008 | comm_code == 1314090009 | comm_code == 1314090010 | comm_code == 1314090012 | comm_code == 1314100004
replace keep_code = 1 if comm_code == 1315060004 | comm_code == 1317040005 | comm_code == 1317040006 | comm_code == 1311000000 | comm_code == 1311010000 | comm_code == 1311010001 | comm_code == 1311010002 | comm_code == 1311010003 | comm_code == 1311010004 | comm_code == 1311010005 | comm_code == 1311010006 | comm_code == 1311010007 | comm_code == 1311010008 | comm_code == 1311010009 | comm_code == 1311010010 | comm_code == 1311010011 | comm_code == 1311010012 | comm_code == 1311010013 | comm_code == 1311010014 | comm_code == 1311010015 | comm_code == 1311010016 | comm_code == 1311010017
replace keep_code = 1 if comm_code == 1311010018 | comm_code == 1311010019 | comm_code == 1311010020 | comm_code == 1311010021 | comm_code == 1311010022 | comm_code == 1311010023 | comm_code == 1318090001 | comm_code == 1314080001 | comm_code == 1314080002 | comm_code == 1315010001 | comm_code == 1312010007 | comm_code == 1312010010 | comm_code == 1310010003 | comm_code == 1101040002 | comm_code == 1101040003 | comm_code == 1301020000 | comm_code == 1301020001 | comm_code == 1301020002 | comm_code == 1310030000 | comm_code == 1310030001 | comm_code == 1310030002 | comm_code == 1310030003
replace keep_code = 1 if comm_code == 1310030004 | comm_code == 1310030005 | comm_code == 1310030006 | comm_code == 1316030000 | comm_code == 1316030001 | comm_code == 1316030002
tab keep_code
drop if keep_code == 0
drop keep_code

save "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\WPI Cleaned.dta"
use "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\WPI Cleaned.dta"



use "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\WPI Prowess Mapped.dta"
rename year10 value10
rename year11 value11
rename year12 value12
rename year13 value13
rename year14 value14
rename year15 value15
rename year16 value16
rename year17 value17
rename year18 value18
rename year19 value19
rename year20 value20
reshape long value, i(NIC5digit WPI_name WPI_comm_code NIC_name) j(year)
tab value
drop year
ren value YR

isid NIC5digit WPI_comm_code YR
sort YR WPI_comm_code
count if missing( WPI_comm_code)
save "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\WPI to Prowess Map Reshaped.dta"
use "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\WPI to Prowess Map Reshaped.dta"
describe WPI_comm_code YR

use "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\WPI Final Cleaned.dta"
describe WPI_comm_code YR
merge 1:m WPI_comm_code YR using "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\WPI to Prowess Map Reshaped.dta"

/*


    Result                           # of obs.
    -----------------------------------------
    not matched                             0
    matched                             2,464  (_merge==3)
    -----------------------------------------

*/

save "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\WPI to Prowess Industry Wise Maping.dta"



*****Cleaning
gen keep_code = 0
destring(WPI_comm_code), replace
replace keep_code = 1 if WPI_comm_code == 1202000010 | WPI_comm_code == 1202000007 | WPI_comm_code == 1301040000 | WPI_comm_code == 1317010000 | WPI_comm_code == 1322010000 | WPI_comm_code == 1304020001 | WPI_comm_code == 1304020004
replace keep_code = 1 if WPI_comm_code == 1314030000 | WPI_comm_code == 1314040000 | WPI_comm_code == 1314050000 | WPI_comm_code == 1310010001 | WPI_comm_code == 1310010002 | WPI_comm_code == 1310010003 | WPI_comm_code == 1310010004
replace keep_code = 1 if WPI_comm_code == 1310010005 | WPI_comm_code == 1310010006 | WPI_comm_code == 1310010007 | WPI_comm_code == 1310010008 | WPI_comm_code == 1310010009 | WPI_comm_code == 1310010010 | WPI_comm_code == 1310010011
replace keep_code = 1 if WPI_comm_code == 1310010012 | WPI_comm_code == 1310010013 | WPI_comm_code == 1310010014 | WPI_comm_code == 1310010015 | WPI_comm_code == 1310010016 | WPI_comm_code == 1310010017 | WPI_comm_code == 1310010018
replace keep_code = 1 if WPI_comm_code == 1310010019 | WPI_comm_code == 1310010020 | WPI_comm_code == 1310010021 | WPI_comm_code == 1310010022 | WPI_comm_code == 1310010023 | WPI_comm_code == 1310010024 | WPI_comm_code == 1310010026
replace keep_code = 1 if WPI_comm_code == 1310010027 | WPI_comm_code == 1310030005 | WPI_comm_code == 1310060003 | WPI_comm_code == 1304010001 | WPI_comm_code == 1304020001 | WPI_comm_code == 1304020004 | WPI_comm_code == 1304030001
replace keep_code = 1 if WPI_comm_code == 1304030002 | WPI_comm_code == 1304040004 | WPI_comm_code == 1304040005 | WPI_comm_code == 1305010001 | WPI_comm_code == 1305010002 | WPI_comm_code == 1305020004 | WPI_comm_code == 1314090003
replace keep_code = 1 if WPI_comm_code == 1314090004 | WPI_comm_code == 1314090007 | WPI_comm_code == 1314090008 | WPI_comm_code == 1314090009 | WPI_comm_code == 1314090010 | WPI_comm_code == 1314090012 | WPI_comm_code == 1314100004
replace keep_code = 1 if WPI_comm_code == 1315060004 | WPI_comm_code == 1317040005 | WPI_comm_code == 1317040006 | WPI_comm_code == 1311010001 | WPI_comm_code == 1311010002 | WPI_comm_code == 1311010003 | WPI_comm_code == 1311010004
replace keep_code = 1 if WPI_comm_code == 1311010005 | WPI_comm_code == 1311010006 | WPI_comm_code == 1311010007 | WPI_comm_code == 1311010008 | WPI_comm_code == 1311010009 | WPI_comm_code == 1311010010 | WPI_comm_code == 1311010011
replace keep_code = 1 if WPI_comm_code == 1311010012 | WPI_comm_code == 1311010013 | WPI_comm_code == 1311010014 | WPI_comm_code == 1311010015 | WPI_comm_code == 1311010016 | WPI_comm_code == 1311010017 | WPI_comm_code == 1311010018
replace keep_code = 1 if WPI_comm_code == 1311010019 | WPI_comm_code == 1311010020 | WPI_comm_code == 1311010021 | WPI_comm_code == 1311010022 | WPI_comm_code == 1311010023 | WPI_comm_code == 1318090001 | WPI_comm_code == 1314080001
replace keep_code = 1 if WPI_comm_code == 1314080002 | WPI_comm_code == 1315010001 | WPI_comm_code == 1310010003 | WPI_comm_code == 1101040002 | WPI_comm_code == 1101040003 | WPI_comm_code == 1301020001 | WPI_comm_code == 1301020002
replace keep_code = 1 if WPI_comm_code == 1310030000 | WPI_comm_code == 1316030001 | WPI_comm_code == 1316030002
drop if keep_code == 0 
drop keep_code

save "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Ordered Merge With WPI.dta"


use "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Ordered Merge With WPI.dta"
replace ca_sales = "." if ca_sales == "NA"
count if missing( ca_sales)
destring(ca_sales), replace
drop if missing( ca_sales)
gen output = ca_sales / yr
order co_code company_name YR ca_export_earnings export_ind output capital_inputs labor_inputs ca_sales  ca_power_and_fuel_exp ca_rawmat_exp ca_gross_fixed_assets avg_wage_rate NIC_name co_nic_code nic_prod_code co_industry_type co_industry_gp_code ca_product_name_mst co_product_gp_code GCF_million_usd Gross_Capital_Formation_clean ca_finance1_year NIC5digit ca_ann_rep_months yr
save "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Ordered Merge With WPI.dta", replace
	
	
	
**** All Commodities Merge for RawMat Expenses

use "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\2011-12 WPI Deflator Cal Year.dta" 
keep if comm_name == "All commodities"
reshape long yr, i(comm_code) j(Year)
tostring( Year), replace
gen YR = substr(Year, 3, 2)
tab YR
drop Year
ren yr rawmat_yr
save "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\All Commodities Deflator for RawMat.dta"

use "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Ordered Merge With WPI.dta"
merge m:1 YR using "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\All Commodities Deflator for RawMat.dta", gen(_merge_AllComm) force

/*
    Result                           # of obs.
    -----------------------------------------
    not matched                             0
    matched                             2,801  (_merge_AllComm==3)
    -----------------------------------------

*/

describe rawmat_yr ca_rawmat_exp
br ca_rawmat_exp
replace ca_rawmat_exp = "." if ca_rawmat_exp == "NA"
count if missing( ca_rawmat_exp)
destring( ca_rawmat_exp), replace
destring( rawmat_yr), replace
gen rawmat = ca_rawmat_exp / rawmat_yr
drop if missing( rawmat)
order co_code company_name YR ca_export_earnings export_ind output capital_inputs labor_inputs rawmat ca_sales ca_power_and_fuel_exp ca_rawmat_exp ca_gross_fixed_assets avg_wage_rate NIC_name co_nic_code nic_prod_code co_industry_type co_industry_gp_code ca_product_name_mst co_product_gp_code GCF_million_usd Gross_Capital_Formation_clean ca_finance1_year NIC5digit ca_ann_rep_months yr rawmat_yr
br rawmat
save "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Ordered Merge With WPI.dta", replace


*****Fuel and Power Deflator for Energy 
use "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\2011-12 WPI Deflator Cal Year.dta"
keep if comm_name == "II FUEL & POWER"
reshape long yr, i(comm_code) j(Year)
tostring( Year), replace
gen YR = substr(Year, 3, 2)
tab YR
drop Year
ren yr energy_yr
save "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Fuel and Power Deflator for Energy.dta"

use "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Ordered Merge With WPI.dta"
merge m:1 YR using "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Fuel and Power Deflator for Energy.dta", gen(_merge_Power_Fuel) force

/*

    Result                           # of obs.
    -----------------------------------------
    not matched                             0
    matched                             2,766  (_merge_Power_Fuel==3)
    -----------------------------------------
*/

br ca_power_and_fuel_exp
replace ca_power_and_fuel_exp = "." if ca_power_and_fuel_exp == "NA"
destring( ca_power_and_fuel_exp), replace
describe energy_yr
destring( energy_yr), replace* Regress raw material inputs on capital and labor to capture productivity shocks
xtreg ln_rawmat_inputs ln_capital_inputs ln_labor_inputs, fe
predict rawmat_hat, xb

****Second-stage regression to estimate the Cobb-Douglas production function
xtreg ln_output ln_capital_inputs ln_labor_inputs rawmat_hat, fe
gen energy_inputs = ca_power_and_fuel_exp / energy_yr
drop if missing( energy_inputs)
ren rawmat rawmat_inputs
order co_code company_name YR ca_export_earni.gs export_ind output capital_inputs labor_inputs rawmat energy_inputs ca_sales ca_power_and_fuel_exp ca_rawmat_exp ca_gross_fixed_assets avg_wage_rate co_nic_code nic_prod_code co_industry_type co_industry_gp_code ca_product_name_mst co_product_gp_code GCF_million_usd Gross_Capital_Formation_clean energy_yr rawmat_yr yr
save "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Ordered Merge With WPI.dta", replace

******Energy Consumption Cleaning
use "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Prowess Energy Consumption.dta" 
destring(YR), replace
keep if inrange(YR, 10, 20)
tab YR, m
save "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Energy Consumption 10-20 Prowess.dta"

use "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Energy Consumption 10-20 Prowess.dta"
tostring(YR), replace
merge m:1 YR using "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Fuel and Power Deflator for Energy.dta"

/*
    Result                           # of obs.
    -----------------------------------------
    not matched                             0
    matched                            72,888  (_merge==3)
    -----------------------------------------
*/

destring( energy_yr), replace
gen energy_input_new = energy_cons_value / energy_yr
drop if missing( energy_input_new)
save "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Merged Energy Cons and Deflator.dta"

use "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Merged Energy Cons and Deflator.dta"
destring(YR), replace
collapse (sum) energy_cons_value (first) company_name energy_name, by( co_code YR)
save "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\Energy Cons.dta"

use "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\rawmat cons.dta"
collapse (sum) rawmat_inputs_new (first) company_name, by( co_code YR)
save "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\rawmat.dta



*******RawMat Consumption Cleaning
destring(YR), replace
keep if inrange(YR, 10, 20)
tab YR, m

use "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\trial.dta"
merge 1:1 co_code YR using "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\do\rawmat.dta", gen(_merge_rawmat)
tab NIC5digit if _merge_rawmat==1
tab NIC5digit export_ind if _merge_rawmat==1
drop if  _merge_rawmat==1
drop if missing( ln_output)
order co_code company_name YR ca_export_earnings export_ind ln_output ln_capital_inputs ln_labor_inputs rawmat_inputs_new energy_inputs_new output capital_inputs labor_inputs rawmat_inputs energy_inputs ca_sales ca_power_and_fuel_exp ca_rawmat_exp ca_gross_fixed_assets avg_wage_rate co_nic_code nic_prod_code co_industry_type co_industry_gp_code ca_product_name_mst co_product_gp_code GCF_million_usd Gross_Capital_Formation_clean energy_yr rawmat_yr yr ln_rawmat_inputs ln_energy_inputs
tab NIC5digit if missing(energy_inputs_new)



*******Final Cleaning
drop if NIC5digit == "10402" & missing(energy_inputs_new)
drop if NIC5digit == "24105" & missing(energy_inputs_new)
drop if NIC5digit == "24202" & missing(energy_inputs_new)
drop if NIC5digit == "20114" & missing(energy_inputs_new)
drop if NIC5digit == "20119" & missing(energy_inputs_new)

drop if missing( ln_rawmat_inputs_new)
order co_code company_name YR ca_export_earnings export_ind ln_output ln_capital_inputs ln_labor_inputs ln_rawmat_inputs_new ln_energy_inputs_new output capital_inputs labor_inputs rawmat_inputs_new energy_inputs_new rawmat_inputs energy_inputs ca_sales ca_power_and_fuel_exp ca_rawmat_exp ca_gross_fixed_assets avg_wage_rate co_nic_code nic_prod_code co_industry_type co_industry_gp_code ca_product_name_mst co_product_gp_code GCF_million_usd Gross_Capital_Formation_clean energy_yr rawmat_yr yr ln_rawmat_inputs ln_energy_inputs



/* -------------------------------------------------------------
   MAIN ESTIMATION
------------------------------------------------------------- */
***** SAMPLE- TOP 16 EXPORTING INDUSTRIES OUT OF 750 TOTAL INDUSTRIES. 2073 FIRMS OUT OF 45K


**** Regressions

cd "C:\Users\aksha\OneDrive - The Pennsylvania State University\Desktop\Revamp\Thesis March 2024\Thesis 2024\out\final output"

xtset co_code YR
ssc install levpet
ssc install xtabond2

*Estimating Production function using Levinsohn and Petrin method
levpet ln_output, free(labor) proxy(ln_rawmat ln_energy) capital(ln_capital)
eststo levpet
predict tfp, omega
esttab levpet using "levpet.csv", se replace

*Creating new variables in order to test for learning-by-exporting hypothesis
*Regress TFP on export intensity and control variables (R&D, size, import intensity)
gen export_intensity = ca_export_earnings / ca_sales  
gen rd_intensity = ca_rnd_exp / ca_sales  
gen rnd_intensity = ca_rnd / ca_sales         

*Testing for Learning-by-Exporting
xtset co_code YR
xtabond2 tfp L.tfp export_intensity rnd_intensity ca_total_assets, gmm(L.tfp export_intensity, lag(1 .)) iv(ca_total_assets rnd_intensity) robust
eststo lbe
xtabond2 export_intensity L.export_intensity tfp rnd_intensity ca_total_assets, gmm(L.export_intensity tfp, lag(2 .)) iv(rnd_intensity ca_total_assets, eq(level)) robust
eststo lbe2
esttab lbe lbe2 using "lbe.csv", se

*Testing for Self-Selection Final
sort co_code YR
logit export_ind tfp rnd_intensity ca_total_assets
eststo ss_logit
probit export_ind tfp rnd_intensity ca_total_assets
eststo ss_probit
xtlogit export_ind tfp rnd_intensity ca_total_assets, fe
eststo ss_xtlogit
esttab ss_logit ss_probit ss_xtlogit using "self_selection.csv", se

*Test Productivity through Export Status Changes
* Create dummy variables for export status transitions
gen start_exporting = (ca_export_earnings > 0 & L.ca_export_earnings == 0)
gen stop_exporting = (ca_export_earnings == 0 & L.ca_export_earnings > 0)
gen continuous_exporting = (ca_export_earnings > 0 & L.ca_export_earnings > 0)
xtreg tfp start_exporting stop_exporting continuous_exporting rnd_intensity ca_total_assets, fe robust
eststo export_status
esttab export_status using "export_status.csv", se

***************