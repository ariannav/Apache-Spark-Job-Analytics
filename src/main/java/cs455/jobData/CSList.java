package cs455.jobData;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

public class CSList{

    private ArrayList<String> csList = new ArrayList<String>(Arrays.asList("java", "c++", "c", "c#", "html", "css", "python", "javascript", "ruby",
        "objective-c", "php", "swift", "sql", "perl", "groovy", "scala", "go", "databases", "algorithms", "algorithm", "artificial", "intelligence",
        "robotics", "network", "networking", "security", "cryptography", "hadoop", "mapreduce", "spark", "apache", "graphics", "parallel", "distributed",
        "meteor", "nodejs", "rubyonrails", "django", "ionic", "boostrap", "wordpress", "drupal", "net", "angularjs", "emberjs", "backbonejs",
        "jquery", "underscore", "mongodb", "redis", "postgresql", "mysql", "oracle", "sqlserver", "junit", "r", "angular", "reactjs", "apachespark",
        "tensorflow", "vuejs", "aspnet", "asp", "nvc", "ajax", "silverlight", "android", "pascal"));

    public CSList(){
    }

    public ArrayList<String> getCSList(){
        return csList;
    }
}
