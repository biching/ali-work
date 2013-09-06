package com.match;

/**************
 * 关键词匹配的UDF函数
 * 输出为每条关键词本身
 * 
 * */
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


public class KWMatch extends UDF{

	public ArrayList<ArrayList<String> >  kw_array = new ArrayList <ArrayList<String> > ();
	public static int flag=0;
	public static final String str_and = "&";
	public static final String str_not = "~";
	public static final String str_field = "\t";
	//public static final String str_delimiter = 001;
	
	public void read_kw(final String kw_path){
		try 
		{	

			BufferedReader br = new BufferedReader(new FileReader (kw_path));			   
		   	String row;
		   	while( (row = br.readLine()) != null){
				String kw_field = row.trim().replace(str_not, str_and+str_not);
				if( null == kw_field || kw_field.length() < 1)
					continue;
				
				String[] kw_atoms = kw_field.split(str_and);
				   ArrayList<String> tmp = new ArrayList<String>();
				   int valid_word_num = 0;
				   for(int i=0; i<kw_atoms.length; i++)
					   {
					   String key_word = kw_atoms[i].trim();
					   if(null != key_word && key_word.length()>=1 )
					   {
						   tmp.add(key_word.toLowerCase());
						   valid_word_num = valid_word_num +1;
					   }
					   }
				   if( valid_word_num>=1 )
					   kw_array.add(tmp);
			   	}
  
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
		
		read_kw(kw_path);
		
		String souce = source_text.trim().toLowerCase();
		
		int kw_num = kw_array.size();
		for (int i = 0; i < kw_num; i++)
		{
			
			
			ArrayList<String> kw = kw_array.get(i);
			int len = kw.size();
			
			String kw_word = null;
			int j=0;
			for (j = 0; j < len; j++)
			{			
				kw_word = kw.get(j).trim();
				if(null == kw_word || kw_word.length()<1) continue;
				
				if(kw_word.length()==1 && str_not == kw_word) continue;
				if(kw_word.length()>=2 && kw_word.substring(0,1).equals(str_not))
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
				String str_output = kw.get(0);
				for (j = 1; j < len; j++)
				{
					str_output = str_output + str_and + kw.get(j);
				}

				return new Text(str_output.replace(str_and+str_not,str_not));
			}
		}
		
		return null;
		
	} //end func:evaluate()

}//end_class
