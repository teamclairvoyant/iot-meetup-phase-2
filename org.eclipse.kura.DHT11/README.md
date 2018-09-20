DHT11 OSGI plugin for Kura IOT gateway
===================================

1.**DESCRIPTION**

This project is java based OSGi Bundle that is installed on Eclipse Kura (An IOT Gateway), to connect DHT11 sensor and get measurement from Sensor and store those reading to Eclipse Kapua(Cloud Services) for analysis purposes.

2.**SETTING THE ENVIRONMENT AND USAGE**

   1. First Setting up the Kura Development Environment in Eclipse. Follow the below given link for complete instruction.
       https://eclipse.github.io/kura/dev/kura-setup.html
       
   2. Try to implement the Hello World example given in Kura's official page(hit below given link) to check your local Kura 	    dev environment is working properly or not.
   
       Hello World Example :- https://eclipse.github.io/kura/dev/hello-example.html
       
       Deploying Bundles(On Remote Target Device) :- https://eclipse.github.io/kura/dev/deploying-bundles.html
       
   3. Our OSGi Bundle is using **Apache Felix Maven Bundle Plugin (BND)** to build and create deployable Jar so it's better 	  to get familiar with this tool. As you may need to change **‘pom.xml’** according to your use case and all the 	       configuration for creating MANIFEST.MF file (part of OSGi bundle) is present in build tag of pom. Please visit below 	  link for more information. 
       
       http://felix.apache.org/documentation/subprojects/apache-felix-maven-bundle-plugin-bnd.html 
       
   4. Now clone the "org.eclipse.kura.DHT11" project from the git repository. 							https://github.com/teamclairvoyant/iot-meetup.git
    
    NOTE : Part of code where you may need to change the configuration.
    • DHT11DataReader.java : Change the value of static variable "pin". Currently it is set to 7 which is the Pin number of Raspberry Pi connected to Data pin of DHT11 sensor.You can use any GPIO pin number based upon your Raspberry Pi.
    
    • /src/main/resources/OSGI-INF/component.xml : Change the target value of Cloud Service Reference to PID of Cloud Services which you have configured in your Kura.
    
   5. Navigate to the place where pom.xml file is present and run the following command. This builds a Deploy-able Jar
      (OSGi Bundle) file with all the dependencies under your local maven repository (or path of bundled JAR will be given on       terminal after successfully executing below command).
       
        mvn clean install       
    
   6. Copy and save the downloaded jar into desktop or any other folder as you will need this to deploy on Kura.
    
   7. Install/Deploy this bundle into your Kura enable remote target device through Eclipse IDE. Please follow below link for       instruction on deploying on remote device through eclipse.
   
       https://eclipse.github.io/kura/dev/deploying-bundles.html#remote-target-device
       
   8. After installing bundle on Kura, bundle can be managed using below commands.
    
     a. First enter into Kura enable remote target device(like Raspberry Pi ) through ssh.
     
          :~$ ssh pi_username@pi_ip_address
	  
	   OR
	   
 	  If the Raspberry Pi is with you then directly open the terminal .

     b. Now pen OSGi console using below command.
     
	  :~$ telnet localhost 5002

     c. List all the installed bundle.
     
	  osgi> ss 
	  
	- Now you could see your installed bundle with ID and State in last of the complete bundle list if it was installed successfully from Eclipse. You can see DHT11 bundle in last.
     
     d. Commands for starting and stopping bundle with IDs..
     
	 osgi> start {Bundle_ID} or stop {Bundle_ID} 

     e. After starting your bundle check your bundle's state like Active/Registered/Unsatisfied. 	
     
	osgi> ls {Bundle_ID}
	
	Note:-  After this command if your bundle doesn't show the "Active" state means there is some problem with your bundle, you can check logs in directory /var/log/ with file named `kura.log` and `kura-console.log` to figure out the exact issue.

     f. Now you can navigate to the directory /var/log/ for checking logs and results coming from your bundle. (You would see Temperature and Humidity reading in `kura.log` file)
     
     g. To exit from Osgi console please use ‘disconnect’ command, do not use ‘exit’ otherwise your Kura service will be stopped.
    
     h. If you don’t see any error on Kura log file then you can go to Kapua console ( location- http://kapua-broker-address:8080/ ) to check the data coming from Sensors thorough this OSGi bundle. Data tab in left panel consists all incoming records of data.

     i. And finally, this data can be used in further downstream application for analysis.  Also, it can be exported into CSV or Excel file. You can also connect Kapua to Grafana to create dashboard for visualizing the data.
    
    
   9. Finally In case we want to show our Sensor data to be displayed on a Dashboard, please follow the steps to Setup Grafana on local system.
   
   	a. How to get started with Grafana(Setup and Installation).
	
	  http://docs.grafana.org/guides/getting_started/
		
        b. How to setup Grafana to display Kapua metrics/data. 
	
         https://www.youtube.com/watch?v=iMiBjzKHBuk
