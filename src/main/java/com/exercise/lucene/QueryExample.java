package com.exercise.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class QueryExample {
    public static void main(String[] args) throws Exception {
        //queryParse();
        queryAPI();
    }

    public static void queryParse() throws Exception {
        String defaultField = "title";
        Path indexPath = Paths.get("E:\\temp\\lucene-data");
        Directory dir = FSDirectory.open(indexPath);
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);
        Analyzer analyzer = new SmartChineseAnalyzer();
        // 查询方式1：查询表达式
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

    public static void queryAPI() throws Exception {
        Path indexPath = Paths.get("E:\\temp\\lucene-data");
        Directory dir = FSDirectory.open(indexPath);
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        Term term = new Term("title", "美国");
        Query termQuery = new TermQuery(term);
        System.out.println("Query: " + termQuery);

        // 返回前10条
        TopDocs tds = searcher.search(termQuery, 10);
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
