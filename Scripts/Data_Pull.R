library(data.table)
library(RODBC)


#Connect to Petrolook Database
con <- odbcConnect("PTRLKTDB",uid = "ptrlk_read_only",pwd = "ptrlkre@d0nly")
EFS_Econ <- data.table(sqlQuery(con, 'SELECT * FROM EFS_LESLIE_ECON'))
EFS_Attri <- data.table(sqlQuery(con, 'SELECT * FROM EFS_LESLIE_ATRI'))
odbcClose(con)


#Rename tables and key
Econ <- data.table(EFS_Econ, key = c('OBJ_ID', 'LINK_ID'))
Attri <- data.table(EFS_Attri, key = c('OBJ_ID', 'LINK_ID'))


#Filter by PDP and join onto econ table
Attri_PDP <- Attri[RESERVES_CATEGORY == 'PDP', ]
PDP_Streams <- Econ[Attri_PDP[, .(LINK_ID, RES_YEAR, QUARTER, OBJ_ID)]]


#Pull in ARIES data
con <- odbcDriverConnect('driver={SQL Server};server=housqldb03;database=Aries;trusted_connection=true')
Aries_Prop <- data.table(sqlQuery(con, 'SELECT * FROM AC_PROPERTY'), key = 'MUWI')


#Merge Aires_Prop and PDP_Streams
master <- merge(PDP_Streams, Aries_Prop[RSV_CAT == '1PDP'], by.x = 'MURPHY_ID', by.y = 'MUWI')


#Clean up
master <- master[is.na(PERIOD) == FALSE, ]
master[, PERIOD := as.Date(PERIOD)]
str(master)

#Write results to csv
#64bit R likes 64 bit Oracle but doesn't like 32bit access.
#Opting for staying in 64 bit and just writing to csv
fwrite(master, file = 'C:/Users/pruetax/OneDrive/Murphy Work/EFS Lookback/EFS_PDP.csv')
