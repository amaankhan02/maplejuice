package test

import (
	"bufio"
	juice_exe_sql "cs425_mp4/cmd/juices/juice_SQL"
	maples_exe_sql_filter "cs425_mp4/cmd/maples/maple_SQL_filter"
	mj "cs425_mp4/internal/maplejuice_exe"
	"fmt"
	"os"
	"testing"
)

func TestMapleSQLFilter(t *testing.T) {
	num_lines := 7
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\maple_sql_filter_test.txt"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)
	regex := "^K" // starting with letter K
	schema := "id,firstname,lastname,age"

	result := maples_exe_sql_filter.MapleSQLFilter(filescanner, regex, num_lines, schema)
	mj.PrintKeyValPairsSQLFilter(result)
}

func TestMapleSQLFilterTrafficData1(t *testing.T) {
	num_lines := 121
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\Traffic_Signal_Intersections.csv"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)
	regex := "Video,Radio"
	schema := "X,Y,OBJECTID,Intersecti,UPS,Coord_Type,CNTRL_Seri,CNTRL_Mode,Number_of_,Detection_,Interconne,Percent_St,Year_Timed,LED_Status,CNTRL_Vers,Cabinet_Ty,CNTRL_Note,Install_Da,Black_Hard,Year_Paint,Countdown_,All_Red_Fl,Condition,ConditionDate,InstallDate,WarrantyDate,LegacyID,FACILITYID,Ownership,OwnershipPercent,LED_Installed_Year,Controller_ID,Notes,RepairYear,FieldVerifiedDate"

	result := maples_exe_sql_filter.MapleSQLFilter(filescanner, regex, num_lines, schema)
	mj.PrintKeyValPairsSQLFilter(result)
}

func TestMapleSQLFilterTrafficData2(t *testing.T) {
	num_lines := 121
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\Traffic_Signal_Intersections.csv"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)

	// . -> any char, // * -> zero or more occurence of the preceding char
	// basically you can have VideoRadio with anything in between

	regex := "Video.*Radio"
	schema := "X,Y,OBJECTID,Intersecti,UPS,Coord_Type,CNTRL_Seri,CNTRL_Mode,Number_of_,Detection_,Interconne,Percent_St,Year_Timed,LED_Status,CNTRL_Vers,Cabinet_Ty,CNTRL_Note,Install_Da,Black_Hard,Year_Paint,Countdown_,All_Red_Fl,Condition,ConditionDate,InstallDate,WarrantyDate,LegacyID,FACILITYID,Ownership,OwnershipPercent,LED_Installed_Year,Controller_ID,Notes,RepairYear,FieldVerifiedDate"

	result := maples_exe_sql_filter.MapleSQLFilter(filescanner, regex, num_lines, schema)
	mj.PrintKeyValPairsSQLFilter(result)
}

func TestMapleSQLFilterTrafficData3(t *testing.T) {
	num_lines := 121
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\Traffic_Signal_Intersections.csv"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)

	// . -> any char, // * -> zero or more occurence of the preceding char
	// basically you can have VideoRadio with anything in between

	regex := "Video|Radio"
	schema := "X,Y,OBJECTID,Intersecti,UPS,Coord_Type,CNTRL_Seri,CNTRL_Mode,Number_of_,Detection_,Interconne,Percent_St,Year_Timed,LED_Status,CNTRL_Vers,Cabinet_Ty,CNTRL_Note,Install_Da,Black_Hard,Year_Paint,Countdown_,All_Red_Fl,Condition,ConditionDate,InstallDate,WarrantyDate,LegacyID,FACILITYID,Ownership,OwnershipPercent,LED_Installed_Year,Controller_ID,Notes,RepairYear,FieldVerifiedDate"

	result := maples_exe_sql_filter.MapleSQLFilter(filescanner, regex, num_lines, schema)
	fmt.Println(len(result))
	mj.PrintKeyValPairsSQLFilter(result)
}

func TestJuiceSQLFilter(t *testing.T) {
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\juice_sql_filter_test.txt"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)
	matched_lines := juice_exe_sql.JuiceSQL(filescanner)

	juice_exe_sql.PrintSliceString(matched_lines)
}

func TestJuiceSQLFilterDemo(t *testing.T) {
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\juice_sql_demo_test.txt"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)
	matched_lines := juice_exe_sql.JuiceSQL(filescanner)

	juice_exe_sql.PrintSliceString(matched_lines)
}
