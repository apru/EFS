library(data.table)
library(RODBC)
library(dplyr)
library(stringr)
library(randomForest)

#Well properties of interest
#   LEASE, MUWI, RSV_CAT, SUB_AREA, FIELD, HORIZONTAL_VERTICAL, SURF_LAT, SURF_LONG, BH_LAT, BH_LONG, 
#   PAD_NAME, OPERATOR, INITIAL_RSV_PRESSURE_PSI, LAT_LENGTH_FT,
#   SPACING_DIST_FT, CMP_START_DATE_ACT, FIRST_PROD_ACT, 
#   FRAC_DESIGN_SIMPLIFIED, PERF_PERF,
#   CLUSTER_SPACING_FT, FRACED_STAGES, AVG_RATE_BPM, AVG_PRESS_PSI, FRAC_VEND, 
#   TOTAL_PROPPANT_LB, MESH_100_LBS, LBS_40_70, LBS_30_50, TOTAL_FLUID_BBL, TOTAL_ACID_BBL


#PullData function pulls Aries Property and Prod data and combines in data.table.
#   writeout: Will it output data to csv?
#   path of CSV
PullData <- function(writeout = TRUE, path = 'C:/Users/pruetax/OneDrive/Murphy Work/EFS Lookback/EFS/Master_Prop.csv') {
    #Pull in ARIES Property data
    con <- odbcDriverConnect('driver={SQL Server};server=housqldb03;database=Aries;trusted_connection=true')
    master_prop <- data.table(sqlQuery(con, 'SELECT * FROM AC_PROPERTY'), key = 'MUWI')
    Aries_Prop <- master_prop[RSV_CAT == '1PDP' & HORIZONTAL_VERTICAL == 'Horizontal' & 
                               OP_NONOP == 'O' & INITIAL_RSV_PRESSURE_PSI > 0 & 
                               SPACING_DIST_FT > 0 & PERF_PERF > 0 & 
                               CLUSTER_SPACING_FT > 0 & FRACED_STAGES > 0 & 
                               AVG_PRESS_PSI > 0,
                         .(PROPNUM, LEASE, MUWI, SUB_AREA, FIELD, PAD_NAME, INITIAL_RSV_PRESSURE_PSI, 
                           SPACING_DIST_FT, 
                           #CMP_START_DATE_ACT, 
                           #FIRST_PROD_ACT,
                           FRAC_DESIGN_SIMPLIFIED, PERF_PERF, CLUSTER_SPACING_FT, FRACED_STAGES, 
                           AVG_RATE_BPM, AVG_PRESS_PSI, 
                           FRAC_VENDOR, 
                           TOTAL_PROPPANT_LB, MESH_100_LBS, LBS_40_70, 
                           LBS_30_50, TOTAL_FLUID_BBL, TOTAL_ACID_BBL)]
    
    
    #Pull in ARIES Production data
    con <- odbcDriverConnect('driver={SQL Server};server=housqldb03;database=Aries;trusted_connection=true')
    master_prod <- data.table(sqlQuery(con, 'SELECT * FROM AC_DAILY'))
    Aries_Daily <- master_prod[, .(PROPNUM, D_DATE, OIL, GAS, WATER, CHOKE)]
    
    
    #Get Max_Prod and Date for each well
    Max_Prod <- Aries_Daily[OIL > 0, .(MAX_PROD = max(OIL)), by = .(PROPNUM)]
    Aries_temp <- merge(Max_Prod, Aries_Daily, by.x = c("MAX_PROD", "PROPNUM"), by.y = c("OIL", "PROPNUM"))
    Max_Prod <- Aries_temp[, .(MAX_P_DATE = min(D_DATE)), by = PROPNUM]
    setkey(Max_Prod, PROPNUM)
    setkey(Aries_Daily, PROPNUM)
    Aries_Daily <- merge(Aries_Daily, Max_Prod)
    
    
    #Make sure to have 180 days of production past Max_Prod
    Aries_Daily[, DateDiff := round(difftime(D_DATE, MAX_P_DATE, units = c("days")))]
    Enough_Prod <- Aries_Daily[, max(DateDiff), by = PROPNUM][V1 >= 180]
    Aries_Daily <- Aries_Daily[Enough_Prod]
    Aries_Daily <- Aries_Daily[DateDiff >= 0 & DateDiff <= 180]
    
    
    #Replace LEASE with well number
    Aries_Prop[, LEASE := str_sub(LEASE, -3, -1)]
    Aries_Prop[, WELL_NUMBER := as.numeric(str_extract(LEASE, "[[:digit:]]+"))]
    Aries_Prop[, LEASE := NULL]
    
    
    #Repalce NA's with 0's
    for (j in names(Aries_Daily))
      set(Aries_Daily, which(is.na(Aries_Daily[[j]])), j, 0)
    for (j in names(Aries_Prop))
      set(Aries_Prop, which(is.na(Aries_Prop[[j]])), j, 0)
    
    
    #Get IP30, IP180 and 150-180 cumprod
    IP30 <- Aries_Daily[DateDiff < 30, .(IP30 = sum(OIL)), by = PROPNUM]
    IP180 <- Aries_Daily[DateDiff < 180, .(IP180 = sum(OIL)), by = PROPNUM]
    IP_Data <- cbind(IP30, IP180[, .(IP180 = IP180)])
    
    
    #Merge IP_Data with Aries_Properties
    Master_Prop <- merge(IP_Data, Aries_Prop, by = "PROPNUM")
    
    
    #Output
    if(writeout){fwrite(Master_Prop, file = path)}
    Master_Prop <- Master_Prop[, .(PROPNUM, FIELD, IP30, IP180, INITIAL_RSV_PRESSURE_PSI, 
                                   SPACING_DIST_FT, FRAC_DESIGN_SIMPLIFIED, PERF_PERF,
                                   CLUSTER_SPACING_FT, FRACED_STAGES, AVG_RATE_BPM, 
                                   AVG_PRESS_PSI, TOTAL_PROPPANT_LB, 
                                   MESH_100_LBS, LBS_40_70, LBS_30_50, TOTAL_FLUID_BBL,
                                   TOTAL_ACID_BBL, WELL_NUMBER)]
    return(Master_Prop)
}


#Aries data with production variables and well/completion properties
Master_Aries <- PullData(writeout = TRUE)


#Closer look into Karnes using RF and variable selection
Field <- Master_Aries[FIELD == 'Karnes'
                      , 3:length(Master_Aries)]
Field <- Field[, .(IP30, IP180, INITIAL_RSV_PRESSURE_PSI, SPACING_DIST_FT, FRAC_DESIGN_SIMPLIFIED,
          PERF_PERF, CLUSTER_SPACING_FT, FRACED_STAGES, AVG_RATE_BPM, AVG_PRESS_PSI, 
          TOTAL_PROPPANT_LB, TOTAL_FLUID_BBL, TOTAL_ACID_BBL)]


#Create Training / Testing sets. Train = 80% of data, Test = 20%
set.seed(5)
Field_train <- sample_n(Field, round(nrow(Field) * .8))
Field_test <- anti_join(Field, Field_train, by = c('IP30', 'IP180'))
Train_x <- Field_train[, 3:length(Field)]
Train30_y <- Field_train[, 1]
Train180_y <- Field_train[, 2]


#Train the RF and see results
RF_thirty <- randomForest(x = Train_x, y = Train30_y$IP30, ntree = 1000, importance = TRUE)
RF_thirty
varImpPlot(RF_thirty)








