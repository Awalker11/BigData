package main;



public class S3testApplication {

   

   public static void main(String[] args) {
        DirectoryChangeListener directoryChangeListener =  new DirectoryChangeListener();
        directoryChangeListener.initDirectoryWatchEvent();
    }

    
}