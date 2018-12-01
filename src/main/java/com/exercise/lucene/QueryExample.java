package com.exercise.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.nio.file.Path;
import java.nio.file.Paths;

public class QueryExample {
    public static void main(String[] args) throws Exception {
        //queryParse();
        queryAPI();
    }

    /**
     * 查询方式1：查询表达式
     * @throws Exception
     */
    public static void queryParse() throws Exception {
        String defaultField = "title";
        Path indexPath = Paths.get("E:\\temp\\lucene-data");
        Directory dir = FSDirectory.open(indexPath);
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);
        Analyzer analyzer = new SmartChineseAnalyzer();
        QueryParser parser = new QueryParser(defaultField, analyzer);
        parser.setDefaultOperator(QueryParser.Operator.AND);
        Query query = parser.parse("content:加大");
        //Query query = parser.parse("id:[ 1 TO 3 ]");
        System.out.println("Query: " + query.toString());

        // 返回前10条
        TopDocs tds = searcher.search(query, 10);
        for (ScoreDoc sd : tds.scoreDocs) {
            // Explanation explanation = searcher.explain(query, sd.doc);
            // System.out.println("explain:" + explanation + "\n");
            Document doc = searcher.doc(sd.doc);
            System.out.println("DocID:" + sd.doc);
            System.out.println("id:" + doc.get("id"));
            System.out.println("title:" + doc.get("title"));
            System.out.println("content:" + doc.get("content"));
            System.out.println("文档评分:" + sd.score);
        }
        dir.close();
        reader.close();
    }

    /**
     * 查询方式2：查询API
     * @throws Exception
     */
    public static void queryAPI() throws Exception {
        Path indexPath = Paths.get("E:\\temp\\lucene-data");
        Directory dir = FSDirectory.open(indexPath);
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);
        //指定项查询
//        Term term = new Term("title", "美国");
//        Query termQuery = new TermQuery(term);
//        System.out.println("Query: " + termQuery);
        //指定int类型的范围查询
//        Query rangeQuery = IntPoint.newRangeQuery("reply", 600, 1000);
//        System.out.println("Query: " + rangeQuery);
        //指定项的范围查询
        TermRangeQuery termRangeQuery = new TermRangeQuery("id", new BytesRef("1".getBytes()), new BytesRef("3".getBytes()), true, true);
        System.out.println("Query: " + termRangeQuery);

        // 返回前10条
        TopDocs tds = searcher.search(termRangeQuery, 10);
        for (ScoreDoc sd : tds.scoreDocs) {
            // Explanation explanation = searcher.explain(query, sd.doc);
            // System.out.println("explain:" + explanation + "\n");
            Document doc = searcher.doc(sd.doc);
            System.out.println("DocID:" + sd.doc);
            System.out.println("id:" + doc.get("id"));
            System.out.println("title:" + doc.get("title"));
            System.out.println("content:" + doc.get("content"));
            System.out.println("文档评分:" + sd.score);
        }
        dir.close();
        reader.close();
    }
}
