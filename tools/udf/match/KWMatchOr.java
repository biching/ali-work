package com.match;

/**************
 * 关键词匹配的UDF函数
 * 输出为特定字段
 * 关键词之间是或者的关系
 * */

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


public class KWMatchR2 extends UDF{
	
	public static int flag=0;
	public static final String str_or = "|";
	public static final String str_field = ",";
	//public static final String str_delimiter = 001;
	
	public  HashMap<String,String> kw_hash = new HashMap<String,String>();
	public  ArrayList<ArrayList<String> >  kw_array = new ArrayList <ArrayList<String> > ();
	
	public void read_kw(final String kw_path){
		try 
		{	

			BufferedReader br = new BufferedReader(new FileReader (kw_path));			   
		   	String row;
		   	
		   	//每条记录代表一组关键词+其它内容
		   	while( (row = br.readLine()) != null){
		   		
		   		//将读入的记录切分开来，读取关键词列（第一列）到kw_field中
		   		String row_content[] = row.trim().split(str_field);
				String kw_field = row_content[0].trim().toLowerCase();
				
				if( null == kw_field || kw_field.length() < 1)
					continue;
				//将关键词和输入记录，存入hashtable中
				if ( kw_hash.containsKey(kw_field) )
					continue;
				else
					kw_hash.put(kw_field,row_content[1]);
				
				//将关键词组切分到tmp中
				String[] kw_atoms = kw_field.split(str_or);
				   ArrayList<String> tmp = new ArrayList<String>();
				   int valid_word_num = 0;
				   for(int i=0; i<kw_atoms.length; i++)
				   {
					   String key_word = kw_atoms[i].trim();
					   if((null != key_word) && key_word.length()>=1 )
					   {
						   tmp.add(key_word);
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
				throw new RuntimeException("file not found,tell me ,where is the kw file");
				
		}
		catch (IOException e){
			throw new RuntimeException("io exception!");
			}
		
	}//end func:read_kw
	
	

	public Text evaluate(final String source_text, final String kw_path){
		
		
		if(null == source_text || "" == source_text || null == kw_path || "" == kw_path)
			return null;
		
		//装载关键词列表	
		read_kw(kw_path);
			
		String souce = source_text.trim().toLowerCase();
		
		int kw_num = kw_array.size();
		for (int i = 0; i < kw_num; i++)
		{
			
			
			ArrayList<String> kw = kw_array.get(i);
			int len = kw.size();
			
			String kw_word = null;
			for (int j = 0; j < len; j++)
			{			
				kw_word = kw.get(j).trim();
				//if(null == kw_word || kw_word.length()<1) continue;
				
				if(souce.indexOf(kw_word) < 0)
					continue;							
                
				//匹配成功，输出结果
				String str_output = kw.get(0);
				for (k = 1; k < len; k++)
				{
					str_output = str_output + str_or + kw.get(k);
				}
				
				return new Text(kw_hash.get(str_output));
			}				

		}
		
		return null;
		
	} //end func:evaluate()

}//end_class
