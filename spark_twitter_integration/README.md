to run this project on window os you need to the following thing
1. download the winutils from here https://github.com/steveloughran/winutils/blob/master/hadoop-2.7.1/bin/winutils.exe .
2. create folder name .../winutils/bin and place winutils.exe into winutils folder //not in bin folder.
3. inside the code change property value named as System.setProperty("hadoop.tmp.dir",".../winutils") //not include bin folder.
4. clone the project and try to run.

#logic for sentiment analysis
I have used stanford core nlp to get sentiment of tweets. The stanford core nlp gives 0(most negative),1(negative),2(neutral),3(positive),4(very positive)
as a sentiment value and we know tweets consist of multiple lines so avg sentiment should be calculated.

hence i have devided this sentiment value into three parts
 case avg_sentiment < 2 negative
 case avg_sentiment = 2 neutral
 case avg_sentiment > 2 positive

