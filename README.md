# utl-number-of-days-between-old-and-new-drug-prescriptions-by-patient-sas-and-sql-r-and-python
Number of days between old and new drug prescriptions by patient sas and sql r and python
    %let pgm=utl-number-of-days-between-old-and-new-drug-prescriptions-by-patient-sas-and-sql-r-and-python;

    %stop_submission;

    Number of days between old and new drug prescriptions by patient sas and sql r and python

    CONTENTS

      1 sas datastep
      2 sas sql
      3 r sql
      4 python sql
      5 sas ascii plot (on end)
      6 related repos (on end)

    github
    https://tinyurl.com/54w7cpk7
    https://github.com/rogerjdeangelis/utl-number-of-days-between-old-and-new-drug-prescriptions-by-patient-sas-and-sql-r-and-python

    SOAPBOX ON
      The input data needs to be sorted by id and code day1 (so day1 is monotonic increasing)
      Also I added rowid to the input for a primary key. Rowid or row_number are
      autocreated in many sql dialects.
    SOAPBOX OFF


    Count records where the next date is within 7 days of previous date sas r and python sqlite

    PROBLEM  (identify gaps less than or equal to 7 days)

      ID_CODE_SEQ  0          5         10  DAYS   15         20         25
                  -+----------+----------+----------+----------+----------+--
                  |               ID_CODE DAY1 DAY2 SPREAD   GAP            |
                  |                4-1-1     1    3   3                     |
                  |                4-1-2     6    9   2    gap_le_7days     |
                  |                4-1-3    11   15   5    gap_le_7days     |
                  |                4-1-4    20   24   .    gap_le_7days     |
                  | day1 day2                                               |
            4-1-1 +   1    3                                                + 4-1-1
                  |         3days   GAP LE 7                                |
                  |         <--->                                           |
            4-1-2 +              6      9                                   + 4-1-2
                  |                 5days                                   |
                  |              <--------->  GAP LE 7                      |
            4-1-3 +                        11       15                      + 4-1-3
                  |                                                         |
                  |                        <----------9days----> GAP LE 7   |
            4-1-4 +                                            20       24  + 4-1-4
                  |                                                         |
                  -+----------+----------+----------+----------+----------+--
                   0          5         10  DAYS   15         20         25


    related to
    https://tinyurl.com/yws4vuuv
    https://communities.sas.com/t5/SAS-Programming/Counting-dates-within-range-of-each-other/m-p/753044#M237281

    */ /**************************************************************************************************************************/
    */ /* INPUT                          | PROCESS                                   | OUTPUT
    */ /* ====                           | =======                                   | ======
    */ /* ID_CODE DAY1 DAY2              | 1 SAS DATASTEP                            |  I         D
    */ /*                                | ==============                            |  D         A S
    */ /*   4-1     1    3               |                                           |  _         Y P
    */ /*   4-1     6    9               | data want ;                               |  C   D  D  2 R
    */ /*   4-1    11   15               |   set sd1.have;                           |  O   A  A  L E      G
    */ /*   4-1    20   24               |   by id_code notsorted;                   |  D   Y  Y  A A      A
    */ /*   5-2     1    2               |   day2lag=lag(day2);                      |  E   1  2  G D      P
    */ /*   5-2    10   14               |   if first.id_code then spread=.;         |
    */ /*   5-3    20   22               |   else do;                                | 4-1  1  3  . .
    */ /*   5-3    30   32               |    spread=day1-day2lag;                   | 4-1  6  9  3 3 gap_le_7days
    */ /*                                |    if spread<=7 then gap='gap_le_7days';  | 4-1 11 15  9 2 gap_le_7days
    */ /* options                        |    else gap="";                           | 4-1 20 24 15 5 gap_le_7days
    */ /*  validvarname=upcase;          |   end;                                    | 5-2  1  2 24 .
    */ /* libname sd1 "d:/sd1";          | run;quit;                                 | 5-2 10 14  2 8
    */ /* data sd1.have;                 |                                           | 5-3 20 22 14 .
    */ /* length rowid 3.                | proc print data=want heading=vertical;    | 5-3 30 32 22 8
    */ /*        id_code $8;;            | run;quit;                                 |
    */ /* input                          |                                           |
    */ /*  id code day1 day2;            |                                           |
    */ /*  id_code=                      | 2 sas sql                                 |
    */ /*    catx('-',id,code);          | =========                                 |  I               G
    */ /* rowid=_n_;                     | proc sql;                                 |  D   C  C  N   N A
    */ /*  keep rowid                    |   create                                  |  _   U  U  X   X P
    */ /*       id_code                  |     table want as                         |  C   R  R  T   T D
    */ /*       day1                     |   select                                  |  O   B  E  B   E A      G
    */ /*       day2;                    |    l.id_code                              |  D   E  N  E   N Y      A
    */ /* cards4;                        |   ,l.day1 as curbeg                       |  E   G  D  G   D S      P
    */ /* 4 1 1 3                        |   ,l.day2 as curend                       |
    */ /* 4 1 6 9                        |   ,r.day1 as nxtbeg                       | 4-1  1  3  6   9 3 gal_le_7days
    */ /* 4 1 11 15                      |   ,r.day2 as nxtend                       | 4-1  6  9 11  15 2 gal_le_7days
    */ /* 4 1 20 24                      |   ,r.day1 - l.day2 as gapdays             | 4-1 11 15 20  24 5 gal_le_7days
    */ /* 5 2 1 2                        |   ,ifc(0<=r.day1-l.day2<=7                | 4-1 20 24  .   . .
    */ /* 5 2 10 14                      |     ,"gal_le_7days"                       | 5-2  1  2 10  14 8
    */ /* 5 3 20 22                      |     ," ") as gap                          | 5-2 10 14  .   . .
    */ /* 5 3 30 32                      |  from                                     | 5-3 20 22 30  32 8
    */ /* ;;;;                           |    sd1.have as l                          | 5-3 30 32  .   . .
    */ /* run;quit;                      |  left join                                |
    */ /*                                |    sd1.have as r                          |
    */ /*                                |  on                                       |
    */ /*                                |    l.id_code = r.id_code and              |
    */ /*                                |    l.rowid = r.rowid - 1                  |
    */ /*                                | ;quit;                                    |
    */ /*                                |                                           |
    */ /*                                |                                           |
    */ /*                                | 3 R SQL                                   |  I
    */ /*                                | =======                                   |  D        N S
    */ /*                                |                                           |  _  R     X P
    */ /*                                | proc datasets lib=sd1                     |  C  O     T R
    */ /*                                |   nolist nodetails;                       |  O  W  E  D E      G
    */ /*                                |  delete want;                             |  D  I  N  A A      A
    */ /*                                | run;quit;                                 |  E  D  D  Y D      P
    */ /*                                |                                           |
    */ /*                                | %utl_rbeginx;                             | 4-1 1  .  1 .
    */ /*                                | parmcards4;                               | 4-1 2  3  6 3 gap_le_7days
    */ /*                                | library(haven)                            | 4-1 3  9 11 2 gap_le_7days
    */ /*                                | library(sqldf)                            | 4-1 4 15 20 5 gap_le_7days
    */ /*                                | source("c:/oto/fn_tosas9x.R")             | 5-2 5  .  1 .
    */ /*                                | options(sqldf.dll                         | 5-2 6  2 10 8
    */ /*                                |     = "d:/dll/sqlean.dll")                | 5-3 7  . 20 .
    */ /*                                | have<-read_sas(                           | 5-3 8 22 30 8
    */ /*                                |    "d:/sd1/have.sas7bdat")                |
    */ /*                                | print(have)                               |
    */ /*                                | want<-sqldf('                             |
    */ /*                                | with dif as (                             |
    */ /*                                |   select                                  |
    */ /*                                |     id_code                               |
    */ /*                                |    ,rowid                                 |
    */ /*                                |    ,lag(day2) over (                      |
    */ /*                                |       partition by id_code                |
    */ /*                                |       order by rowid                      |
    */ /*                                |     ) as end,                             |
    */ /*                                |     day1 as nxtday                        |
    */ /*                                |   from have                               |
    */ /*                                | )                                         |
    */ /*                                | select                                    |
    */ /*                                |   id_code                                 |
    */ /*                                |  ,rowid                                   |
    */ /*                                |  ,end                                     |
    */ /*                                |  ,nxtday                                  |
    */ /*                                |  ,nxtday - end as spread                  |
    */ /*                                |  ,case                                    |
    */ /*                                |    when (nxtday - end)<=7                 |
    */ /*                                |     then "gap_le_7days"                   |
    */ /*                                |     else " "                              |
    */ /*                                |   end as gap                              |
    */ /*                                | from dif                                  |
    */ /*                                | order by rowid;                           |
    */ /*                                | ')                                        |
    */ /*                                | want                                      |
    */ /*                                | fn_tosas9x(                               |
    */ /*                                |       inp    = want                       |
    */ /*                                |      ,outlib ="d:/sd1/"                   |
    */ /*                                |      ,outdsn ="want"                      |
    */ /*                                |      )                                    |
    */ /*                                | ;;;;                                      |
    */ /*                                | %utl_rendx;                               |
    */ /*                                |                                           |
    */ /*                                | proc print data=sd1.want                  |
    */ /*                                |  heading=vertical;                        |
    */ /*                                | run;quit;                                 |
    */ /*                                |                                           |
    */ /*                                |                                           |
    */ /*                                | 4 PYTHON SQL                              |
    */ /*                                | ============                              |  I
    */ /*                                |                                           |  D        N S
    */ /*                                | proc datasets lib=sd1                     |  _  R     X P
    */ /*                                |   nolist nodetails;                       |  C  O     T R
    */ /*                                |  delete pywant;                           |  O  W  E  D E      G
    */ /*                                | run;quit;                                 |  D  I  N  A A      A
    */ /*                                |                                           |  E  D  D  Y D      P
    */ /*                                | %utl_pybeginx;                            |
    */ /*                                | parmcards4;                               | 4-1 1  .  1 .
    */ /*                                | exec(open( \                              | 4-1 2  3  6 3 gap_le_7days
    */ /*                                |  'c:/oto/fn_pythonx.py').read());         | 4-1 3  9 11 2 gap_le_7days
    */ /*                                | have,meta = \                             | 4-1 4 15 20 5 gap_le_7days
    */ /*                                |  ps.read_sas7bdat(                        | 5-2 5  .  1 .
    */ /*                                |   'd:/sd1/have.sas7bdat');                | 5-2 6  2 10 8
    */ /*                                | want=pdsql('''                            | 5-3 7  . 20 .
    */ /*                                |   with dif as (                           | 5-3 8 22 30 8             |
    */ /*                                |     select                                |
    */ /*                                |       id_code                             |
    */ /*                                |      ,rowid                               |
    */ /*                                |      ,lag(day2) over (                    |
    */ /*                                |         partition by id_code              |
    */ /*                                |         order by rowid                    |
    */ /*                                |       ) as end,                           |
    */ /*                                |       day1 as nxtday                      |
    */ /*                                |     from have                             |
    */ /*                                |   )                                       |
    */ /*                                |   select                                  |
    */ /*                                |     id_code                               |
    */ /*                                |    ,rowid                                 |
    */ /*                                |    ,end                                   |
    */ /*                                |    ,nxtday                                |
    */ /*                                |    ,nxtday - end as spread                |
    */ /*                                |    ,case                                  |
    */ /*                                |      when (nxtday - end)<=7               |
    */ /*                                |       then "gap_le_7days"                 |
    */ /*                                |       else " "                            |
    */ /*                                |     end as gap                            |
    */ /*                                |   from dif                                |
    */ /*                                |   order by rowid;                         |
    */ /*                                |    ''');                                  |
    */ /*                                | print(want);                              |
    */ /*                                | fn_tosas9x(want                           |
    */ /*                                |   ,outlib='d:/sd1/'                       |
    */ /*                                |   ,outdsn='pywant'                        |
    */ /*                                |   ,timeest=3);                            |
    */ /*                                | ;;;;                                      |
    */ /*                                | %utl_pyendx;                              |
    */ /*                                |                                           |
    */ /*                                | proc print data=                          |
    */ /*                                |    sd1.pywant                             |
    */ /*                                |    heading=vertical;                      |
    */ /*                                | run;quit;                                 |
    */ /**************************************************************************************************************************/
    /*___                                   _ _         _       _
    | ___|   ___  __ _ ___    __ _ ___  ___(_|_)  _ __ | | ___ | |_
    |___ \  / __|/ _` / __|  / _` / __|/ __| | | | `_ \| |/ _ \| __|
     ___) | \__ \ (_| \__ \ | (_| \__ \ (__| | | | |_) | | (_) | |_
    |____/  |___/\__,_|___/  \__,_|___/\___|_|_| | .__/|_|\___/ \__|
                                                 |_|
    */

    options validvarname=upcase;
    libname sd1 "d:/sd1";
    data sd1.have;
    input
     id code day1 day2;
     id_code_seq=catx('-',id,code,_n_);
     id_code=catx('-',id,code);
     ltr1=put(day1,2.);
     ltr2=put(day2,2.);
     keep code id id_code_seq id_code ltr1 ltr2 day1 day2;
    cards4;
    4 1 1 3
    4 1 6 9
    4 1 11 15
    4 1 20 24
    5 2 1 2
    5 2 10 14
    5 3 20 22
    5 3 30 32
    ;;;;
    run;quit;

    options ls=64 ps=24;
    options FORMCHAR='|----|+|---+=|-/\<>*' ;
    proc plot data=sd1.have(where=(id=4));
      plot id_code_seq*day1=' ' $ ltr1 id_code_seq*day2=' ' $ ltr2/
          overlay box vreverse;
    run;quit;
    options ls=255 ps=66;

    /**************************************************************************************************************************/
    /*|   Plot of ID_CODE_SEQ*DAY1$LTR1.  Symbol used is ' '.                                                                 */
    /*|   Plot of ID_CODE_SEQ*DAY2$LTR2.  Symbol used is ' '.                                                                 */
    /*|                                                                                                                       */
    /*|            ---+------------+------------+------------+---                                                             */
    /*| D_CODE_SEQ |                                            |                                                             */
    /*|      4-1-1 +    1  3                                    +                                                             */
    /*|            |                                            |                                                             */
    /*|            |                                            |                                                             */
    /*|      4-1-2 +           6   9                            +                                                             */
    /*|            |                                            |                                                             */
    /*|            |                                            |                                                             */
    /*|      4-1-3 +                11    15                    +                                                             */
    /*|            |                                            |                                                             */
    /*|            |                                            |                                                             */
    /*|      4-1-4 +                            20   24         +                                                             */
    /*|            |                                            |                                                             */
    /*|            ---+------------+------------+------------+---                                                             */
    /*|               0           10           20           30                                                                */
    /**************************************************************************************************************************/

    /*__              _       _           _
     / /_    _ __ ___| | __ _| |_ ___  __| |  _ __ ___ _ __   ___  ___
    | `_ \  | `__/ _ \ |/ _` | __/ _ \/ _` | | `__/ _ \ `_ \ / _ \/ __|
    | (_) | | | |  __/ | (_| | ||  __/ (_| | | | |  __/ |_) | (_) \__ \
     \___/  |_|  \___|_|\__,_|\__\___|\__,_| |_|  \___| .__/ \___/|___/
                                                      |_|
    */

    https://github.com/rogerjdeangelis/utl-compute-lagged-differences-of-row-sums-by-group
    https://github.com/rogerjdeangelis/utl-lagging-variables-without-renaming-the-variables
    https://github.com/rogerjdeangelis/utl-lags-in-proc-sql-monotonic-datastep-is-preferred
    https://github.com/rogerjdeangelis/utl-lowest-order-polynomial-that-goes-through-a-set-of-points-lagrange-interpolation
    https://github.com/rogerjdeangelis/utl-make-new-column-with-the-previous-version-of-a-row-wps-hash-lag-and-r-code
    https://github.com/rogerjdeangelis/utl-select-date-minus-lag-date-in-days-by-group-using-sas-and-sql-in-r-and-python-muti-language
    https://github.com/rogerjdeangelis/utl-select-obs-where-current-and-previous-values-differ-by-group-for-multiple-variables-r-sql-lag
    https://github.com/rogerjdeangelis/utl-timeseries-calcualtion-of-acf-and-pacf-lagged-autocorrelations

    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */
