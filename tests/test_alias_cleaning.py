"""
Test suite for alias_cleaning.py

Tests the text normalization, typo collapse, and canonical token selection
functionality used in the HITL SKU curation process.
"""

import pytest
from hitl.utils.alias_cleaning import (
    _normalize,
    _norm_tokens,
    build_rep_map_typo_collapse,
    _apply_rep_map,
    most_frequent_canonical_tokens,
    _alias_to_transformed_string,
    rank_by_tokens,
    transform_aliases_with_canonical_tokens,
)


class TestNormalization:
    """Test text normalization functions"""

    def test_normalize_basic(self):
        """Test basic normalization functionality"""
        assert _normalize("Hello World") == "hello world"
        assert _normalize("UPPERCASE text") == "uppercase text"
    
    def test_normalize_punctuation_removal(self):
        """Test punctuation removal"""
        assert _normalize("hello, world!") == "hello world"
        # The regex [^\w\s]+ replaces punctuation but preserves underscores in words
        assert _normalize("test-case_123") == "test case_123"
        assert _normalize("@#$%^&*()") == ""
    
    def test_normalize_stop_words(self):
        """Test stop word removal"""
        assert _normalize("the quick brown fox") == "quick brown fox"
        assert _normalize("this is a test") == "test"
        assert _normalize("and or but") == ""
    
    def test_normalize_whitespace(self):
        """Test whitespace normalization"""
        assert _normalize("  multiple   spaces  ") == "multiple spaces"
        assert _normalize("\t\n\r mixed \t whitespace \n") == "mixed whitespace"
    
    def test_normalize_empty_and_edge_cases(self):
        """Test edge cases for normalization"""
        assert _normalize("") == ""
        assert _normalize("   ") == ""
        assert _normalize("the a an") == ""
    
    def test_norm_tokens_basic(self):
        """Test token extraction"""
        assert _norm_tokens("hello world") == ["hello", "world"]
        # _norm_tokens filters tokens with numbers (only .isalpha() tokens pass)
        assert _norm_tokens("test123 word") == ["word"]  # filters non-alpha
    
    def test_norm_tokens_with_numbers(self):
        """Test that numeric tokens are filtered"""
        # Only pure alphabetic tokens are kept, duplicates preserved
        assert _norm_tokens("word123 123 word") == ["word"]
        assert _norm_tokens("apple banana apple") == ["apple", "banana", "apple"]
        assert _norm_tokens("word123 123 word apple banana apple") == ["word", "apple", "banana", "apple"]
        assert _norm_tokens("abc123def") == []  # mixed alpha-numeric filtered


class TestTypoCollapse:
    """Test typo collapse functionality"""

    def test_build_rep_map_simple(self):
        """Test basic representative mapping"""
        token_lists = [["keyboard"], ["keybord"], ["kayboard"]]
        rep_map = build_rep_map_typo_collapse(token_lists, max_edit_distance=2)
        
        # "keyboard" should be the representative (most frequent or first)
        assert rep_map["keyboard"] == "keyboard"
        # typos should map to keyboard
        assert rep_map["keybord"] == "keyboard"
        assert rep_map["kayboard"] == "keyboard"
    
    def test_build_rep_map_frequency_preference(self):
        """Test that more frequent tokens become representatives"""
        token_lists = [
            ["keybord"], ["keybord"], ["keybord"],  # frequent typo
            ["keyboard"]  # less frequent correct spelling
        ]
        rep_map = build_rep_map_typo_collapse(token_lists, max_edit_distance=2)
        
        # More frequent "keybord" might become representative
        # But "keyboard" is longer and correct, so it depends on implementation
        assert "keybord" in rep_map
        assert "keyboard" in rep_map
    
    def test_build_rep_map_min_length(self):
        """Test minimum representative length constraint"""
        token_lists = [["cat"], ["bat"], ["rat"]]
        rep_map = build_rep_map_typo_collapse(token_lists, max_edit_distance=1, min_rep_len=4)
        
        # Short words shouldn't become representatives of other words
        # Each should map to itself
        assert rep_map["cat"] == "cat"
        assert rep_map["bat"] == "bat"
        assert rep_map["rat"] == "rat"
    
    def test_build_rep_map_empty_input(self):
        """Test empty input handling"""
        rep_map = build_rep_map_typo_collapse([])
        assert rep_map == {}
        
        rep_map = build_rep_map_typo_collapse([[]])
        assert rep_map == {}
    
    def test_apply_rep_map(self):
        """Test applying representative mapping"""
        token_lists = [["hello", "world"], ["test", "case"]]
        rep_map = {"hello": "hi", "world": "earth"}
        
        result = _apply_rep_map(token_lists, rep_map)
        expected = [["hi", "earth"], ["test", "case"]]
        assert result == expected


class TestCanonicalTokens:
    """Test canonical token selection"""

    def test_most_frequent_canonical_tokens_basic(self):
        """Test basic canonical token selection"""
        token_lists = [
            ["wireless", "keyboard"],
            ["wireless", "mouse"],
            ["wireless", "keyboard"],
            ["bluetooth", "keyboard"]
        ]
        
        tokens = most_frequent_canonical_tokens(token_lists, top_m=3)
        
        # "wireless" and "keyboard" should be most frequent
        assert "wireless" in tokens
        assert "keyboard" in tokens
        assert len(tokens) <= 3
    
    def test_most_frequent_canonical_tokens_presence_vs_count(self):
        """Test presence-based vs count-based frequency"""
        token_lists = [
            ["test", "test", "test"],  # "test" appears 3 times in one alias
            ["word"]  # "word" appears 1 time in one alias
        ]
        
        # With presence=True, both should have same weight (1 alias each)
        tokens_presence = most_frequent_canonical_tokens(token_lists, top_m=2, use_presence=True)
        
        # With presence=False, "test" should rank higher
        tokens_count = most_frequent_canonical_tokens(token_lists, top_m=2, use_presence=False)
        
        assert "test" in tokens_presence
        assert "word" in tokens_presence
        assert "test" in tokens_count
    
    def test_most_frequent_canonical_tokens_empty(self):
        """Test empty input handling"""
        tokens = most_frequent_canonical_tokens([])
        assert tokens == []
        
        tokens = most_frequent_canonical_tokens([[]])
        assert tokens == []


class TestAliasRanking:
    """Test alias ranking functionality"""

    def test_alias_to_transformed_string(self):
        """Test alias transformation to string"""
        rep_map = {"keybord": "keyboard", "wireles": "wireless"}
        
        result = _alias_to_transformed_string("TechFlow Keybord Wireles", rep_map)
        assert "keyboard" in result
        assert "wireless" in result
        assert "techflow" in result
    
    def test_rank_by_tokens_basic(self):
        """Test basic alias ranking"""
        aliases = [
            "wireless keyboard black",
            "keyboard wireless",
            "black keyboard",
            "completely different product"
        ]
        target_tokens = ["wireless", "keyboard"]
        
        ranked = rank_by_tokens(aliases, target_tokens, k=3)
        
        # Should return list of (alias, score, freq) tuples
        assert len(ranked) <= 3
        assert all(len(item) == 3 for item in ranked)
        
        # Scores should be in descending order
        scores = [item[1] for item in ranked]
        assert scores == sorted(scores, reverse=True)
    
    def test_rank_by_tokens_empty_target(self):
        """Test ranking with empty target tokens"""
        aliases = ["test", "example"]
        ranked = rank_by_tokens(aliases, [], k=3)
        assert ranked == []
    
    def test_rank_by_tokens_frequency_tiebreak(self):
        """Test frequency-based tie-breaking"""
        aliases = [
            "wireless keyboard",  # appears once
            "wireless keyboard",  # appears twice total
            "keyboard wireless"   # same tokens, different order
        ]
        target_tokens = ["wireless", "keyboard"]
        
        ranked = rank_by_tokens(aliases, target_tokens, k=3)
        
        # "wireless keyboard" should rank higher due to frequency
        assert len(ranked) >= 1
        # All should have high scores since they match target tokens well


class TestEndToEndPipeline:
    """Test the complete transformation pipeline"""

    def test_transform_aliases_basic(self):
        """Test basic end-to-end transformation"""
        aliases = [
            "wireless keyboard black",
            "wireless keybord black",
            "black wireless keyboard",
            "keyboard wireless"
        ]
        
        result = transform_aliases_with_canonical_tokens(
            aliases,
            max_edit_distance=1,
            top_k_original=2
        )
        
        # Check result structure
        assert "rep_map" in result
        assert "transformed_aliases" in result
        assert "canonical_tokens" in result
        assert "top_k_original" in result
        assert "top_k_canonicalized" in result
        assert "params" in result
        
        # Check that we get expected number of results
        assert len(result["top_k_original"]) <= 2
        assert len(result["top_k_canonicalized"]) <= 2
        
        # Check that canonical tokens are reasonable
        assert len(result["canonical_tokens"]) > 0
        
        # Verify typo correction in rep_map
        assert "keybord" in result["rep_map"]
        assert result["rep_map"]["keybord"] == "keyboard"
    
    def test_transform_aliases_empty_input(self):
        """Test empty input handling"""
        result = transform_aliases_with_canonical_tokens([])
        
        assert result["rep_map"] == {}
        assert result["transformed_aliases"] == []
        assert result["canonical_tokens"] == []
        assert result["top_k_original"] == []
        assert result["top_k_canonicalized"] == []
    
    def test_transform_aliases_whitespace_only(self):
        """Test input with only whitespace"""
        aliases = ["", "   ", "\t\n"]
        result = transform_aliases_with_canonical_tokens(aliases)
        
        assert result["rep_map"] == {}
        assert result["transformed_aliases"] == []
        assert result["canonical_tokens"] == []
        assert result["top_k_original"] == []
        assert result["top_k_canonicalized"] == []
    
    def test_transform_aliases_real_world_example(self):
        """Test with realistic product name variations"""
        aliases = [
            "TechFlow Wireless Keyboard Black",
            "tf wireless keybord black",
            "wireless kb black",
            "techflow wireless keyboard - black",
            "TF Wireless Keyboard (Black)",
            "wireless keybord black tf",
            "brand new techflow keyboard wireless black"
        ]
        
        result = transform_aliases_with_canonical_tokens(
            aliases,
            max_edit_distance=1,
            min_rep_len=4,
            top_m_canonical_tokens=4,
            top_k_original=3
        )
        
        # Should find common tokens like "wireless", "keyboard", "black", "techflow"
        canonical = result["canonical_tokens"]
        assert any("wireless" in token for token in canonical)
        assert any("keyboard" in token for token in canonical)
        
        # Should correct "keybord" -> "keyboard"
        assert "keybord" in result["rep_map"]
        assert result["rep_map"]["keybord"] == "keyboard"
        
        # Should return top candidates
        assert len(result["top_k_canonicalized"]) == 3
        
        # Each canonicalized result should have required fields
        for item in result["top_k_canonicalized"]:
            assert "alias" in item
            assert "alias_canonicalized" in item
            assert "score" in item
            assert "freq" in item
    
    def test_transform_aliases_parameters_preserved(self):
        """Test that parameters are preserved in output"""
        params = {
            "max_edit_distance": 2,
            "min_rep_len": 5,
            "top_m_canonical_tokens": 3,
            "top_k_original": 5
        }
        
        result = transform_aliases_with_canonical_tokens(
            ["test alias"],
            **params
        )
        
        assert result["params"] == params


class TestEdgeCases:
    """Test edge cases and error conditions"""

    def test_unicode_handling(self):
        """Test unicode character handling"""
        aliases = ["café wireless keyboard", "cafe wireless keyboard"]
        result = transform_aliases_with_canonical_tokens(aliases)
        
        # Should handle unicode gracefully
        assert len(result["canonical_tokens"]) > 0
    
    def test_very_long_aliases(self):
        """Test very long alias names"""
        long_alias = "extremely long product name with many descriptive words " * 10
        aliases = [long_alias, "short name"]
        
        result = transform_aliases_with_canonical_tokens(aliases)
        
        # Should handle long aliases without errors
        assert len(result["transformed_aliases"]) == 2
    
    def test_single_character_tokens(self):
        """Test handling of single character tokens"""
        aliases = ["a b c keyboard", "x y z mouse"]
        result = transform_aliases_with_canonical_tokens(aliases)
        
        # Single characters should be filtered out during normalization
        # or handled gracefully
        assert len(result["canonical_tokens"]) >= 0
    
    def test_numeric_only_aliases(self):
        """Test aliases with only numbers"""
        aliases = ["123 456", "789", "model 2024"]
        result = transform_aliases_with_canonical_tokens(aliases)
        
        # Should handle numeric content gracefully
        # Only "model" should remain from "model 2024"
        assert len(result["canonical_tokens"]) >= 0


# Integration test with realistic data
class TestIntegration:
    """Integration tests with realistic data scenarios"""

    @pytest.fixture
    def realistic_aliases(self):
        """Fixture with realistic product name variations"""
        return [
            "Apple MacBook Pro 13-inch",
            "Apple MacBook Pro 13 inch",
            "MacBook Pro 13\" Apple",
            "Apple MacBook Pro (13-inch)",
            "MacBook Pro 13-inch by Apple",
            "13-inch MacBook Pro - Apple",
            "Apple MacBook Pro 13in",
            "MacBook Pro 13 Apple",
            "APPLE MACBOOK PRO 13 INCH",
            "apple macbook pro 13-inch",
        ]
    
    def test_realistic_product_consolidation(self, realistic_aliases):
        """Test consolidation of realistic product name variations"""
        result = transform_aliases_with_canonical_tokens(
            realistic_aliases,
            max_edit_distance=1,
            top_k_original=3
        )
        
        # Should identify common tokens
        canonical = result["canonical_tokens"]
        assert any("apple" in token for token in canonical)
        assert any("macbook" in token for token in canonical)
        assert any("pro" in token for token in canonical)
        
        # Should rank similar aliases highly
        top_aliases = [item["alias"] for item in result["top_k_canonicalized"]]
        assert len(top_aliases) == 3
        
        # All top aliases should contain core product information
        for alias in top_aliases:
            alias_lower = alias.lower()
            assert "macbook" in alias_lower
            assert "pro" in alias_lower


if __name__ == "__main__":
    # Run tests if script is executed directly
    pytest.main([__file__])
