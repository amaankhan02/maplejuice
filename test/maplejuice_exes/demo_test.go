package test

//
//import (
//	"bufio"
//
//	juice_exe "cs425_mp4/cmd/juices/juice_demo_phase2"
//	maples_exe_word_count "cs425_mp4/cmd/maple_word_count"
//
//	_ "cs425_mp4/cmd/maples/maple_demo_phase2"
//	"fmt"
//	"os"
//	"testing"
//)
//
//func TestMapleDemoPhase1(t *testing.T) {
//	num_lines := 27
//	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\Traffic_Signal_Intersections.csv"
//	file, err := os.Open(file_path)
//	if err != nil {
//		fmt.Errorf("Error opening file: %s", err)
//	}
//	defer file.Close()
//
//	// Create a scanner to read the file line by line
//	filescanner := bufio.NewScanner(file)
//	X := "Fiber"
//	schema := "X,Y,OBJECTID,Intersecti,UPS,Coord_Type,CNTRL_Seri,CNTRL_Mode,Number_of_,Detection_,Interconne,Percent_St,Year_Timed,LED_Status,CNTRL_Vers,Cabinet_Ty,CNTRL_Note,Install_Da,Black_Hard,Year_Paint,Countdown_,All_Red_Fl,Condition,ConditionDate,InstallDate,WarrantyDate,LegacyID,FACILITYID,Ownership,OwnershipPercent,LED_Installed_Year,Controller_ID,Notes,RepairYear,FieldVerifiedDate"
//
//	result := maple_demo_phase1.MapleDemoPhase1(filescanner, num_lines, X, schema)
//	maple_demo_phase1.PrintKeyValuePairs(result)
//}
//
//func TestJuiceDemoPhase1(t *testing.T) {
//	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\juice_demo_phase1_test.txt"
//	file, err := os.Open(file_path)
//	if err != nil {
//		fmt.Errorf("Error opening file: %s", err)
//	}
//	defer file.Close()
//
//	// Create a scanner to read the file line by line
//	filescanner := bufio.NewScanner(file)
//	result := juice_demo_phase1.JuiceDemoTest1(filescanner)
//
//	maples_exe_word_count.PrintKeyValuePairs(result)
//}
//
//func TestMapleDemoPhase2(t *testing.T) {
//	num_lines := 4
//	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\maple_demo_phase2_test.txt"
//	file, err := os.Open(file_path)
//	if err != nil {
//		fmt.Errorf("Error opening file: %s", err)
//	}
//	defer file.Close()
//
//	// Create a scanner to read the file line by line
//	filescanner := bufio.NewScanner(file)
//
//	result := MapleDemoPhase2(filescanner, num_lines)
//	maple_demo_phase2.PrintKeyValuePairs(result)
//}
//
//func TestJuiceDemoPhase2(t *testing.T) {
//	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\juice_demo_phase2_test.txt"
//	file, err := os.Open(file_path)
//	if err != nil {
//		fmt.Errorf("Error opening file: %s", err)
//	}
//	defer file.Close()
//
//	// Create a scanner to read the file line by line
//	filescanner := bufio.NewScanner(file)
//	_ = juice_exe.JuicePhase2(filescanner)
//
//	//maple_demo_phase2.PrintKeyValuePairs(result)
//}
