package com.match;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class MatchType extends UDF {
	
	public static ArrayList<ArrayList<String> >  kw_array = new ArrayList <ArrayList<String> > ();
	public static int flag=0;
	public static final String str_and = "&";
	public static final String str_not = "~";
	public static final String str_delimiter = "\t";
	
	public void read_kw(final String kw_path){
		try 
		{

			BufferedReader br = new BufferedReader(new FileReader (kw_path));			   
		   	String row;
			while( (row = br.readLine()) != null){

				String row_trim = row.trim().replace(str_not, str_and+str_not);
				if( null == row_trim || row_trim.length() < 1)
					continue;
				
				//每一行的第四个元素是关键词列
				String[] line_fields = row_trim.split(str_delimiter);
				String[] kw_atoms = line_fields[3].trim().toLowerCase().split(str_and);
				ArrayList<String> tmp = new ArrayList<String>();
				
				//将brand、model、和价格附到前面
				tmp.add(line_fields[1]);
				tmp.add(line_fields[2]);
				tmp.add(line_fields[4]);
				int valid_word_num = 0;
				for(int i=0; i<kw_atoms.length; i++){
					   
					String key_word = kw_atoms[i].trim();
					if(null != key_word && key_word.length()>=1 ){
						   tmp.add(key_word);
						   valid_word_num = valid_word_num +1;
					}
				}
				if( valid_word_num>=1 )
					   kw_array.add(tmp);
				   
			 }//end_while
  
			   br.close();		
		}
		catch (FileNotFoundException e) 
		{
				throw new RuntimeException("file not found");
				
		}
		catch (IOException e){
			throw new RuntimeException("io exception!");
			}
		
	}//end func:read_kw
	
	public Text evaluate(final String source_text, final String kw_path){
		
		if(null == source_text || "" == source_text || null == kw_path || "" == kw_path)
			return null;
		
		//只初始化一次关键词列表，用构造函数应该也能实现，还没有测试.需要在构造函数中调用evaluate。
		if(0==flag){
			read_kw(kw_path);
			flag++;
		}
		
		String souce = source_text.trim().toLowerCase();
		//发现用python处理更简单一点，暂用python处理，
		int kw_num = kw_array.size();
		for (int i = 3; i < kw_num; i++)
		{
			
			
			ArrayList<String> kw = kw_array.get(i);
			int len = kw.size();
			
			String kw_word = null;
			int j=0;
			for (j = 0; j < len; j++)
			{			
				kw_word = kw.get(j).trim();
				if(null == kw_word || kw_word.length()<1) continue;
				
				if(kw_word.length()==1 && "~" == kw_word) continue;
				if(kw_word.length()>=2 && kw_word.substring(0,1).equals("~"))
				{
					if(kw_word.substring(1).trim().length()>=1 && souce.indexOf(kw_word.substring(1).trim()) >= 0)
					{
						break;
					}
				}
				else if(souce.indexOf(kw_word) < 0)
					break;							
			}
			if(j==len)
			{
				return new Text(kw.toString());
			}
		}
		
		return null;
		
	} //end evaluate()

}//end_class
