# this script will generate the key value pairs you need to use for counting number of words


# loop through first 20 lines
# create a dictionary for that
# after you are done, you want to add those key value pairs to the list

# repeat this for the next 20 lines

def generate_key_val_pairs(file_path, process_num_lines):

    # list of tuples
    # first_val = word; second_val = word_count
    word_to_word_count = []

    with open(file_path, 'r') as file:

        dict1 = {}
        for line_num, line in enumerate(file):

            # Split the line into words
            line_words = line.split()

            # loop through these words and add to dictionary
            for word in line_words: 
                if word in dict1:
                    dict1[word] += 1
                else:
                    dict1[word] = 1
            
            # Every 20 lines or at the end of the file
            if ((line_num % (process_num_lines - 1) == 0) and line_num != 0) or line_num == (total_lines_in_file - 1):
                # append all elements in dict1 in word_to_word_count
                for key, val in dict1.items():
                    word_to_word_count.append((key, val))
                    dict1 = {}

    return word_to_word_count


    # read file 



if __name__ == "__main__":
    total_lines_in_file = 25
   
    # you are supposed to get this file path from sdfs
    file_path = "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\scripts\\input_data_25.txt"
    process_num_lines = 10 # number can be between 20 - 100

    word_to_word_count = generate_key_val_pairs(file_path, process_num_lines)

    for word in word_to_word_count:
        print(word)



'''
	//// list of tuples
	//// first_val = word; second_val = word_count
	//word_to_word_count := []Tuple_Word_Count{}
	//
	//// Open the file
	//file, err := os.Open(file_path)
	//if err != nil {
	//	fmt.Println("Error opening file:", err)
	//	return nil
	//}
	//defer file.Close()
	//
	//// Create a scanner to read the file line by line
	//scanner := bufio.NewScanner(file)
	//
	//map1 := make(map[string]int)
	//line_num := 1
	//
	//// Iterate through each line
	//for scanner.Scan() {
	//	line := scanner.Text()
	//
	//	// Split the line into words
	//	words := strings.Fields(line)
	//
	//	// loop through these words and add it to the dictionary
	//	for _, word := range words {
	//		_, exists := map1[word]
	//
	//		if exists {
	//			map1[word] += 1
	//		} else {
	//			map1[word] = 1
	//		}
	//	}
	//
	//	// if you reach the end of batch or end of file, add every element from dictionary to list of tuples
	//	if (line_num%process_num_lines == 0) || (line_num >= total_lines_in_file) {
	//		// append all elements from the map to word_to_word_count
	//		for word, word_count := range map1 {
	//			word_to_word_count = append(word_to_word_count, Tuple{word, word_count})
	//		}
	//
	//		// reinitialize to empty map
	//		map1 = make(map[string]int)
	//	}
	//
	//	line_num += 1
	//}
	//
	//// Check for errors during scanning
	//if err := scanner.Err(); err != nil {
	//	fmt.Println("Error reading file:", err)
	//}
	//
	//return word_to_word_count
'''
