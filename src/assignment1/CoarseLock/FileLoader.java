package assignment1.CoarseLock;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by ronnygeo on 9/24/16.
 */
public class FileLoader {
    public static List load(String filename) throws FileNotFoundException {
        List<String> lines = new ArrayList<>();
        Scanner reader = new Scanner(new File(filename));

        while (reader.hasNext()) {
           lines.add(reader.nextLine());
        }

        return lines;
    }
}
