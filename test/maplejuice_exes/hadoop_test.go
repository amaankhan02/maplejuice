package test

import (
	"bufio"
	"cs425_mp4/Hadoop/mapper1"
	"cs425_mp4/Hadoop/mapper2"
	"cs425_mp4/Hadoop/reducer1"
	"cs425_mp4/Hadoop/reducer2"
	"fmt"
	"os"
	"testing"
)

func TestMapper1(t *testing.T) {
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\Traffic_Signal_Intersections.csv"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)
	X := "Fiber"
	schema := "X,Y,OBJECTID,Intersecti,UPS,Coord_Type,CNTRL_Seri,CNTRL_Mode,Number_of_,Detection_,Interconne,Percent_St,Year_Timed,LED_Status,CNTRL_Vers,Cabinet_Ty,CNTRL_Note,Install_Da,Black_Hard,Year_Paint,Countdown_,All_Red_Fl,Condition,ConditionDate,InstallDate,WarrantyDate,LegacyID,FACILITYID,Ownership,OwnershipPercent,LED_Installed_Year,Controller_ID,Notes,RepairYear,FieldVerifiedDate"

	mapper1.Mapper1(filescanner, X, schema)
}

func TestReducer1(t *testing.T) {
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\mapper1_output.txt"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)

	reducer1.Reducer1(filescanner)
}

func TestMapper2(t *testing.T) {
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\reducer1_output.txt"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)

	mapper2.Mapper2(filescanner)
}

func TestReducer2(t *testing.T) {
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\mapper2_output.txt"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)

	reducer2.Reducer2(filescanner)

	// FINAL OUTPUT:
	// ,1.6129031
	//None,12.903225
	//Radar,1.6129031
	//Loop/None,3.2258062
	//,1.6129031
	//Loop/Video,6.4516125
	//Loop,38.709675
	//Video,33.870968
}
