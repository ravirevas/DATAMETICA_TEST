(1,111,Electronics,Click)
(2,112,Fashion,Click)
(3,113,Kids,AddtoCart)
(4,114,Food,Purchase)
(5,115,Books,Logout)                                                                    --sample data
(6,114,Food,Click)
(7,113,Kids,AddtoCart)
(8,115,Books,Purchase)
(9,111,Electronics,Click)
(10,112,Fashion,Purchase)
(3,112,Fashion,Click)
(1,112,Fashion,Click)
(2,112,Electronics,Click)


A = LOAD '/home/cloudera/Desktop/data' USING PigStorage(',') AS (userID:chararray,pID:chararray,category:chararray,action:chararray);
B = FILTER A BY (category=='Fashion');
C = FILTER A BY (category=='Electronics');
E = JOIN B BY userID FULL OUTER,C BY userID;                                             -- joining fashion and category
HALF_RIGHT = FILTER E BY $0 is NULL;
HALF_LEFT = FILTER E BY $4 is NULL;
HALF_LEFT_NO_NONE = FOREACH HALF_LEFT GENERATE $0,$2;                                    --removing none from fashion
HALF_RIGHT_NO_NONE = FOREACH HALF_RIGHT GENERATE $4,$6;                                  --removing none from electronics
FINAL_RESULT = UNION HALF_LEFT_NO_NONE,HALF_RIGHT_NO_NONE;                               --joining after none removal
DUMP FINAL_RESULT;



(3,Fashion)                                                                             --result
(10,Fashion)
(9,Electronics)