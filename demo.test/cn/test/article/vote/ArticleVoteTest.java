package cn.test.article.vote;

import java.util.Map;

import org.junit.Test;

import cn.zwz.article.vote.ArticleVote;

public class ArticleVoteTest {

	@Test
	public void testPostArticle() {
		ArticleVote articleVote = new ArticleVote();
		
		String id = articleVote.postArticle("zwz", "帝吧招募白班小吧主", "http://tieba.baidu.com/p/3365391002");
		System.out.println(id);
		
		Map<String, String> article = articleVote.getArticleById(id);
		
		articleVote.printArticle(article);
	}
	
	@Test
	public void testAddVote() {
		ArticleVote articleVote = new ArticleVote();
		String articleId = "article:1";
		boolean isVote = articleVote.addVote("dongxiaoyi", articleId);
		if(isVote){
			Map<String, String> article = articleVote.getArticleById(articleId.substring(articleId.indexOf(':') + 1));
			articleVote.printArticle(article);
		}

		
		
	}

}
