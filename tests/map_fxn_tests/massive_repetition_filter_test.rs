extern crate datamap_rs;
use datamap_rs::map_fxn::{MassiveWebRepetitionFilter};

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{VecDeque, HashMap};
    use std::hash::{Hash, Hasher};
    use std::collections::hash_map::DefaultHasher;
    
    const EPSILON: f32 = 0.0001; // For floating point comparisons
    
    fn assert_float_eq(a: f32, b: f32) {
        assert!((a - b).abs() < EPSILON, "Expected {}, got {}", b, a);
    }

    #[test]
    fn test_empty_input_ngram_size_1() {
        // Special case: ngram_size = 1 and total_ngrams = 0
        // This happens when the input is empty
        let elements: Vec<&str> = vec![];
        let result = MassiveWebRepetitionFilter::_rep_counter_fraction(&elements, 1, false).unwrap();
        assert_float_eq(result, 1.0);
    }
    
    #[test]
    fn test_single_element_ngram_size_1() {
        // Special case: ngram_size = 1 and total_ngrams = 1
        // This happens when there's only one element
        let elements: Vec<&str> = vec!["hello"];
        let result = MassiveWebRepetitionFilter::_rep_counter_fraction(&elements, 1, false).unwrap();
        assert_float_eq(result, 0.0);
    }
    
    #[test]
    fn test_fewer_elements_than_ngram_size() {
        // Special case: ngram_size > 1 and total_ngrams = 0
        // This happens when there are fewer elements than ngram_size
        let elements: Vec<&str> = vec!["hello"];
        let result = MassiveWebRepetitionFilter::_rep_counter_fraction(&elements, 2, false).unwrap();
        assert_float_eq(result, 0.0);
    }
    
    #[test]
    fn test_ngram_size_1_unweighted_no_repetitions() {
        // No repetitions
        let elements: Vec<&str> = vec!["a", "b", "c", "d"];
        let result = MassiveWebRepetitionFilter::_rep_counter_fraction(&elements, 1, false).unwrap();
        assert_float_eq(result, 0.0);
    }
    
    #[test]
    fn test_ngram_size_1_unweighted_some_repetitions() {
        // Some repetitions
        let elements: Vec<&str> = vec!["a", "b", "a", "c", "b", "d"];
        // "a" appears twice, "b" appears twice, out of 6 elements -> 4/6 = 2/3
        let result = MassiveWebRepetitionFilter::_rep_counter_fraction(&elements, 1, false).unwrap();
        assert_float_eq(result, 4.0/6.0);
    }
    
    #[test]
    fn test_ngram_size_1_unweighted_all_repetitions() {
        // All repetitions
        let elements: Vec<&str> = vec!["a", "a", "a", "a"];
        let result = MassiveWebRepetitionFilter::_rep_counter_fraction(&elements, 1, false).unwrap();
        assert_float_eq(result, 1.0);
    }
    
    #[test]
    fn test_ngram_size_1_unweighted_mixed_length_strings() {
        // Mixed length strings
        let elements: Vec<&str> = vec!["short", "looooong", "short", "medium"];
        // "short" appears twice out of 4 elements -> 2/4 = 0.5
        let result = MassiveWebRepetitionFilter::_rep_counter_fraction(&elements, 1, false).unwrap();
        assert_float_eq(result, 0.5);
    }
    
    #[test]
    fn test_ngram_size_1_weighted_no_repetitions() {
        // No repetitions
        let elements: Vec<&str> = vec!["a", "b", "c", "d"];
        let result = MassiveWebRepetitionFilter::_rep_counter_fraction(&elements, 1, true).unwrap();
        assert_float_eq(result, 0.0);
    }
    
    #[test]
    fn test_ngram_size_1_weighted_equal_length_repetitions() {
        // Some repetitions with equal lengths
        let elements: Vec<&str> = vec!["aa", "bb", "aa", "cc", "bb", "dd"];
        // "aa" appears twice (4 chars), "bb" appears twice (4 chars)
        // Total char length is 12, repeated char length is 8
        let result = MassiveWebRepetitionFilter::_rep_counter_fraction(&elements, 1, true).unwrap();
        assert_float_eq(result, 8.0/12.0);
    }
    
    #[test]
    fn test_ngram_size_1_weighted_all_repetitions() {
        // All repetitions
        let elements: Vec<&str> = vec!["a", "a", "a", "a"];
        let result = MassiveWebRepetitionFilter::_rep_counter_fraction(&elements, 1, true).unwrap();
        assert_float_eq(result, 1.0);
    }
    
    #[test]
    fn test_ngram_size_1_weighted_mixed_length_strings() {
        // Mixed length strings
        let elements: Vec<&str> = vec!["short", "looooong", "short", "medium"];
        // "short" appears twice (10 chars total)
        // Total char length is 5+8+5+6=24, repeated char length is 10
        let result = MassiveWebRepetitionFilter::_rep_counter_fraction(&elements, 1, true).unwrap();
        assert_float_eq(result, 10.0/24.0);
    }
    
    #[test]
    fn test_ngram_size_2_no_repetitions() {
        // ngram_size = 2, no repetitions
        let elements: Vec<&str> = vec!["a", "b", "c", "d", "e"];
        let result = MassiveWebRepetitionFilter::_rep_counter_fraction(&elements, 2, false).unwrap();
        assert_float_eq(result, 0.0);
    }
    
    #[test]
    fn test_ngram_size_2_with_repetition() {
        // ngram_size = 2, with repetition
        let elements: Vec<&str> = vec!["a", "b", "c", "a", "b", "d"];
        // The ngram "a,b" appears twice, each with length 2
        // Total char length is 6, repeated is 2*2=4
        let result = MassiveWebRepetitionFilter::_rep_counter_fraction(&elements, 2, false).unwrap();
        assert_float_eq(result, 4.0/6.0);
    }
    
    #[test]
    fn test_ngram_size_3_with_repetition() {
        // ngram_size = 3, with repetition
        let elements: Vec<&str> = vec!["a", "b", "c", "d", "a", "b", "c", "e"];
        // The ngram "a,b,c" appears twice, each with length 3
        // Total char length is 8, repeated is 2*3=6
        let result = MassiveWebRepetitionFilter::_rep_counter_fraction(&elements, 3, false).unwrap();
        assert_float_eq(result, 6.0/8.0);
    }
    
    #[test]
    fn test_ngram_size_4_with_mixed_length_strings() {
        // ngram_size = 4, with repetition and different length strings
        let elements: Vec<&str> = vec!["short", "a", "b", "c", "short", "a", "b", "d"];
        // The ngram "short,a,b,c" appears once, "short,a,b,d" appears once
        // But "short,a,b" is common with total length (5+1+1)=7, appearing in 2 places
        // Total char length is 5+1+1+1+5+1+1+1=16, but most common repeated ngram is not clear
        // Let's calculate the expected result...
        let mut ngram: VecDeque<String> = VecDeque::with_capacity(4);
        let mut ngram_counts: HashMap<(u64, usize), Vec<usize>> = HashMap::new();
        let mut ngram_char_len = 0;
        
        for (idx, element) in elements.iter().enumerate() {
            ngram.push_back(element.to_string());
            ngram_char_len += element.len();
            if ngram.len() >= 4 {
                let mut hasher = DefaultHasher::new();
                ngram.hash(&mut hasher);
                let hash_val: u64 = hasher.finish();
                ngram_counts.entry((hash_val, ngram_char_len)).or_insert(Vec::new()).push(idx);
                ngram_char_len -= ngram.pop_front().unwrap().len();
            }
        }
        
        let expected = 0.0;
        let result = MassiveWebRepetitionFilter::_rep_counter_fraction(&elements, 4, false).unwrap();
        assert_float_eq(result, expected);
    }
    
    #[test]
    fn test_ngram_size_4_with_overlap() {
        let elements: Vec<&str> = vec!["a", "a", "a", "a", "a", "a", "b", "c", "d", "f"];
        let result = MassiveWebRepetitionFilter::_rep_counter_fraction(&elements, 4, false).unwrap();
        assert_float_eq(result, 6.0/10.0);
    }
    
    #[test]
    fn test_ngram_size_6_no_repetition() {
        // ngram_size = 6, with no repetition
        let elements: Vec<&str> = vec!["a", "b", "c", "d", "e", "f", "g", "h"];
        let result = MassiveWebRepetitionFilter::_rep_counter_fraction(&elements, 6, false).unwrap();
        assert_float_eq(result, 0.0);
    }
    
    #[test]
    fn test_ngram_size_8_with_repetition_and_different_length_strings() {
        // ngram_size = 8, with repetition and different length strings
        let elements: Vec<&str> = vec![
            "the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog",
            "the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "cat"
        ];
        // The sequence "the,quick,brown,fox,jumps,over,the,lazy" is repeated
        // So indices 0-7 and 9-16 are in the repeat set
        // Total length is sum of all string lengths
        // Repeated length is the sum of the repeated strings
        let repeated_idxs: Vec<usize> = [0,1,2,3,4,5,6,7,9,10,11,12,13,14,15,16].to_vec();
        let repeated_len: usize = repeated_idxs.into_iter().map(|v| elements[v].len()).sum::<usize>();
        let total_len: usize = elements.iter().map(|v| v.len()).sum::<usize>();
        let expected = repeated_len as f32 / total_len as f32;
        
        let result = MassiveWebRepetitionFilter::_rep_counter_fraction(&elements, 8, false).unwrap();
        assert_float_eq(result, expected);
    }
    
    #[test]
    fn test_exactly_ngram_size_elements() {
        // Edge case: exactly ngram_size elements
        let elements: Vec<&str> = vec!["a", "b", "c"];
        let result = MassiveWebRepetitionFilter::_rep_counter_fraction(&elements, 3, false).unwrap();
        println!("RESULT {:?}", result);
        assert_float_eq(result, 0.0);
    }
    
    #[test]
    fn test_just_over_ngram_size_elements_with_repetition() {
        // Edge case: just over ngram_size elements, with repetition
        let elements: Vec<&str> = vec!["a", "b", "c", "a", "b", "c"];
        let result = MassiveWebRepetitionFilter::_rep_counter_fraction(&elements, 3, false).unwrap();
        // The ngram "a,b,c" appears twice, each with length 3
        // Total char length is 6, repeated is 2*3=6
        assert_float_eq(result, 1.0);
    }
    
    #[test]
    fn test_very_large_strings() {
        // Edge case: very large strings
        let large_str = "a".repeat(10000);
        let elements: Vec<&str> = vec![&large_str, "b", &large_str];
        let result = MassiveWebRepetitionFilter::_rep_counter_fraction(&elements, 1, true).unwrap();
        // large_str appears twice, total length is 2*10000+1, repeated is 2*10000
        assert_float_eq(result, 20000.0/20001.0);
    }
    
    #[test]
    fn test_realistic_text_bigram_repetition() {
        // Realistic example with repeated phrases
        let text = "to be or not to be that is the question to be or not to be I don't know the answer.";
        let elements: Vec<&str> = text.split_whitespace().collect();

        // For ngram_size = 2, let's manually calculate
        let total_len = elements.iter().map(|v| v.len()).sum::<usize>();

        // The bigram "To be" appears twice (5 chars each)
        // The bigram "be or" appears twice (5 chars each)
        // The bigram "or not" appears twice (6 chars each)
        // The bigram "not to" appears twice (6 chars each)
        // The bigram "to be" appears twice (5 chars each)
        // The bigram "be ," appears once and "be ," appears once, they don't match
        // The most common is either "or not" or "not to" with 6*2=12 chars
        
        let result = MassiveWebRepetitionFilter::_rep_counter_fraction(&elements, 2, false).unwrap();
        assert_float_eq(result, 16.0/total_len as f32);
        }
    
    #[test]
    fn test_performance_with_large_input() {
        // Create a large input with some repetitions
        let mut large_input = Vec::with_capacity(10000);
        for i in 0..5000 {
            large_input.push(format!("word{}", i % 1000));
        }
        
        let elements: Vec<&str> = large_input.iter().map(|s| s.as_str()).collect();
        
        // This should complete without timing out
        let result = MassiveWebRepetitionFilter::_rep_counter_fraction(&elements, 10, false);
        assert!(result.is_ok());
    }
}