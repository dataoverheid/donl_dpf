"use strict";




//-----------------------------------------------------------------------------
//
//  LOAD MODULES
//
//-----------------------------------------------------------------------------

//Load CKAN module
var CKAN = require('ckan');

//Load the request module for easy GET requests (Drupal Taxonomy)
var request = require('request');

//Filesystem to write logs
var fs = require('fs');

var nodemailer      = require('nodemailer');        // For sending email







//-----------------------------------------------------------------------------
//
//  SETTINGS
//
//-----------------------------------------------------------------------------



//Context to run this script in, this could be loaded from a JSON!
var context = {

    beta_acc_admin : {
        api : 'http://beta-acc.data.overheid.nl/data', 
        apikey : 'xxxxx',
		send_mails_to_ckan_user : false
    },
    
    beta_acc_dpf : {
        api : 'http://beta-acc.data.overheid.nl/data', 
        apikey : 'xxxxx',
		send_mails_to_ckan_user : false
    },
    
    prod_admin : {
        api : 'https://data.overheid.nl/data', 
        apikey : 'xxxxx',
		send_mails_to_ckan_user : true
    },
    
    prod_dpf : {
        api : 'https://data.overheid.nl/data', 
        apikey : 'xxxxx',
		send_mails_to_ckan_user : true
    }

    
}

//Ignore SSL certificate errors
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";


//The total amount pf source packages
var numberOfSourcePackages;

//Arrays to hold all source / target packages
var sourcePackages = [];
var targetPackages = [];

//Array to hold the objects (packages) to send
var sendPackages = [];

//Array to hold the packages that need to be deleted
var deletePackages = [];
var numberOfDeletePackages;

//To keep track of what needs to be processed/send in a batch
var processFrom;
var processTo;
var sendFrom;
var sendTo;
var sendToDel;
var sendFromDel;

//Todo:Global
var ckan_donl_admin;


//Number of packages to process a batch in a sleep is done (to prevent this script from crashing the server)
var stepSize = 5;
//How long to sleep inbetween batches
var sleepTime = 500;

//-----------------------------------------------------------------------------
//
//  FUNCTIONS
//
//-----------------------------------------------------------------------------


//A simple sleep function, this is blocking!
function sleep(milliseconds) {
	logger.log( "Sleeping for " + milliseconds + "ms", LOG_TYPE_TRACE);
	
	var start = new Date().getTime();
	for (var i = 0; i < 1e7; i++) {
		if ((new Date().getTime() - start) > milliseconds){
			break;
		}
	}
}


//Logger class
var LOG_TYPE_ERR      = 0;
var LOG_TYPE_WARN   = 1;
var LOG_TYPE_INFO     = 3;
var LOG_TYPE_DEBUG  = 4;
var LOG_TYPE_TRACE 	 = 5;

var logger = function() { 
    
	var maxLevel = LOG_TYPE_INFO;
	
	this.mainLog = [];
	
	this.log = function( line, type ) { 
		
        //Create a timestamp
        var d = new Date();
		
		var ts =  ("0" + d.getDate()).slice(-2) + "-" +  
					   ("0"+(d.getMonth()+1)).slice(-2)+ "-" +  
					  d.getFullYear() + 
					  "T" +
					  ("0"+d.getHours()).slice(-2) + ":" + 
					  ("0"+d.getMinutes()).slice(-2) + ":" + 
					  ("0"+d.getSeconds()).slice(-2);
        
		if( type <= maxLevel ) {
			console.log( ts + "\t" + line );
			this.mainLog.push( ts + " - " + line );
		}
		
	}
	
	this.mappingLogs = [];
    
    this.appendToMappingLog = function( mapping, lookup, message, source_pckg ) {
        
        var d = {
             mappingTable      : mapping, 
             lookupValue       : lookup, 
             logMessage        : message,
             source_package_id : source_pckg
        }
        this.mappingLogs.push( d );
        
    };


    this.validationLogs = {};
	this.logValidationErrors = function( result, id ) {
		if (result.success) {
			return;
		}

		var error = result.error;
		if (error.__type !== "Validation Error") {
			return;
		}

		var validationLogs = this.validationLogs;

		Object.keys(error).forEach(function(key) {
			if (key !== "__type") {
				var d = {
					field: key,
					message: error[key][0],
					id: id
				};

				var dictionaryKey = d.field + d.message;
				if (validationLogs[dictionaryKey] === undefined) {
					validationLogs[dictionaryKey] = [];
				}
				validationLogs[dictionaryKey].push(d);
			}
		});
	};

	this.mailValidationLogs = function() {
		var validationLogs = this.validationLogs;
		var keys = Object.keys(validationLogs);

		if (keys.length == 0) {
			console.log("No validation logs - don't send a mail.");
            return;
        }

		var transporter = nodemailer.createTransport(smtpConfig);
		var body = "";

		keys.forEach(function(key) {
			var validationLog = validationLogs[key];

			var validationMessage = validationLog[0].message + ' (' + validationLog[0].field + ')';
			var validationDatasets = validationLog.map(function(val) {return val.id;});

			body += "<p>" + validationMessage + ":</p>\n";
			body += "<ul>\n";
			body += validationDatasets.map(function(val) { return "<li>" + val + "</li>";}).join("\n");
			body += "</ul>\n";

			console.log(validationMessage + ':\n' + validationDatasets.join("\n"));
			console.log('\n');
		});

		if (body != "") {
			var header = "<p>Beste data-eigenaar,</p>";
			header += "<p>Tijdens het automatisch importeren van uw datasets naar data.overheid.nl, zijn &eacute;&eacute;n of meerdere datasets geweigerd. Dit komt doordat deze datasets niet voldoen aan de juiste waarden om correct te worden weergegeven op data.overheid.nl. In deze mail vindt u informatie over de datasets die zijn geweigerd en welke waarden hierbij aangepast dienen te worden om ze correct te importeren (de meldingen komen direct uit het systeem en kunnen derhalve in het Engels zijn):</p>"

			var footer = "<p>Wij willen graag waarborgen dat alle imports op de juiste wijze worden weergegeven. Daarom verzoeken we u om bovenstaande datasets aan te passen, zodat deze voldoen aan de gehanteerde waarden van data.overheid.nl. Op deze manier kunnen wij uw datasets correct importeren en worden deze op de juiste wijze zichtbaar en vindbaar gemaakt op data.overheid.nl.</p>";
			footer += "<p>Indien u nog vragen heeft of verdere ondersteuning wenst, dan helpen wij u graag via het volgende mailadres: data@koop.overheid.nl.</p>";
			footer += "<p>Met vriendelijke groet,<br/>Team data.overheid.nl</p>";

			validatieMailConfig.html = header + body + footer;
			transporter.sendMail(
				validatieMailConfig,
				function(error, info){
					if(error){
						return console.log(error);
					}
					console.log('Message sent: ' + info.response);
				}
			);
		}
	};


    this.finalize = function() {
        
        //
        //MAIN
        //
        var logMain = {};
        logMain.lines = this.mainLog;
		//fs.writeFile( '/var/data/donl_node/dataplatform_importer/logs/DPF_' + contextName + '_log.json', JSON.stringify(logMain), function (err) { if(err) { console.log("WriteFile Error Main Log"); console.log(err); console.log(); } });
        fs.writeFile( 'logs/DPF_' + contextName + '_log.json', JSON.stringify(logMain), function (err) { if(err) { console.log("WriteFile Error Main Log"); console.log(err); console.log(); } });
        

		// VALIDATION LOGS
		this.mailValidationLogs();

        //
        //MAPPINGS
        //
        
        //Remove all duplicate entries form the mapping logs (we only need to know this once)
        //But do count how many times a certain message occurs
        /*var uMappingLogIds = {};
        for( var i in this.mappingLogs ) {
            if( uMappingLogIds[JSON.stringify(this.mappingLogs[i])] ) {
                uMappingLogIds[JSON.stringify(this.mappingLogs[i])].amount++;
            }
            else {
                uMappingLogIds[JSON.stringify(this.mappingLogs[i])] = {};
                uMappingLogIds[JSON.stringify(this.mappingLogs[i])].index = i;
                uMappingLogIds[JSON.stringify(this.mappingLogs[i])].amount = 1;
            }   
        }
        var uMappingLogs = [];
        for( var i in uMappingLogIds ) {
            this.mappingLogs[ uMappingLogIds[i].index ].amount = uMappingLogIds[i].amount;
            uMappingLogs.push ( this.mappingLogs[ uMappingLogIds[i].index ]);
        }*/
        
		var uMappingLogs = {};
        for( var i in this.mappingLogs ) {
            var id = this.mappingLogs[i].mappingTable + "_" + this.mappingLogs[i].lookupValue;
			//console.log("Generated ID + " + id);
			
			if( uMappingLogs[id] ) {
                uMappingLogs[id].amount++;
            }
            else {
                uMappingLogs[id] = {};
                uMappingLogs[id].index = i;
                uMappingLogs[id].amount = 1;
				
				uMappingLogs[id].mappingTable        = this.mappingLogs[i].mappingTable;
				uMappingLogs[id].lookupValue         = this.mappingLogs[i].lookupValue;
				uMappingLogs[id].logMessage          = this.mappingLogs[i].logMessage;
				uMappingLogs[id].source_package_id   = this.mappingLogs[i].source_package_id;
            }   
        }
		
		
        //
        //Write log to a nice HTML file
        //
        
        //Create a timestamp
        var d = new Date();
        //For display
        var ts = d.getFullYear() + "-" + ("0"+(d.getMonth()+1)).slice(-2) + "-" + ("0" + d.getDate()).slice(-2) + " T" + ("0"+d.getHours()).slice(-2) + ":" + ("0"+d.getMinutes()).slice(-2) + ":" + ("0"+d.getSeconds()).slice(-2);
        
        //For filename
        var tsFile = d.getFullYear() + "-" + ("0"+(d.getMonth()+1)).slice(-2) + "-" + ("0" + d.getDate()).slice(-2) + "T" + ("0"+d.getHours()).slice(-2) + "-" + ("0"+d.getMinutes()).slice(-2) + "-" + ("0"+d.getSeconds()).slice(-2);
        
        
        var css = 'html, body { font-size: 12px; font-family: "RijksOverheidSans", "Droid Sans", Arial, Verdana, sans-serif; }';
        css += 'h1, h2, h3 { font-weight: bold; color: #154273; }';
        css += 'h1 { margin-bottom: 14px; padding-bottom: 1px; font-size: 1.8em; letter-spacing: 2px; }';
        css += 'h2 { margin: 2.5em 0 1em 0em; font-size: 1.3em; letter-spacing: 1.2px; }';
        css += 'li { list-style : none; }';
        css += 'ul.source { margin-bottom:15px; } ';
        css += 'li.logmessage { border-bottom: 1px solid #555555; margin-bottom:15px; } ';
        
        var logFileContent = "<!doctype html><html><head><title>Dataplatform import log for " + ts + "</title><style>" + css + "</style></head><body>";
        
        logFileContent += "<h2>Mapping Logs</h2>";
        logFileContent += "<ul class='logmessage'>";
        for( var i in uMappingLogs ) {
            logFileContent += "<li class='logmessage'>";
            logFileContent += "<div class='label Mapping'><strong>Mapping : </strong>" + uMappingLogs[i].mappingTable + "</div>";
            logFileContent += "<div class='label Waarde'><strong>Waarde : </strong>" + uMappingLogs[i].lookupValue + "</div>";
            logFileContent += "<div class='label LogBericht'><strong>Log bericht : </strong>" + uMappingLogs[i].logMessage + "</div>";
            logFileContent += "<div class='label Bron'><strong>Bron : </strong></div>";
            logFileContent += "<ul class='source'>";
            logFileContent += "<li class='source'>ID : " + uMappingLogs[i].source_package_id + "</li>";
            logFileContent += "<li class='source'>Site : <a href='https://www.dataplatform.nl/dataset/"+uMappingLogs[i].source_package_id + "' target='_new'>https://www.dataplatform.nl/dataset/" + uMappingLogs[i].source_package_id + "</a></li>";
            logFileContent += "<li class='source'>JSON : <a href='https://ckan.dataplatform.nl/api/3/action/package_show?id="+uMappingLogs[i].source_package_id + "' target='_new'>https://ckan.dataplatform.nl/api/3/action/package_show?id=" + uMappingLogs[i].source_package_id + "</a></li>";
            logFileContent += "</ul>";
            logFileContent += "</li>";
        }
        
        logFileContent += "</ul>";
        
        logFileContent += "</body></html>";
        
        //fs.writeFile( '/var/data/donl_node/dataplatform_importer/html_logs/mappings_dataplatform_' + tsFile + '.html', logFileContent, function (err) { if(err) { console.log("WriteFile Error"); console.log(err); console.log(); } });
		fs.writeFile( 'html_logs/mappings_dataplatform_' + tsFile + '.html', logFileContent, function (err) { if(err) { console.log("WriteFile Error"); console.log(err); console.log(); } });
        
    }
	
    
}





/*
 *  GETTING DATA FROM THE SOURCE & TARGET API's
 *
 */


//Get all source packages
function getPackageList() {
    
	logger.log( "Getting source package list", LOG_TYPE_INFO);
	
	//Get the package list
    ckan_dpf.action('package_list', {  }, function( gplErr, gplResult) {
		//Check for errors
        if( !gplErr ) {
			
			//gplResult.result.splice( 12 );
			
			//No errors, store information
        	numberOfSourcePackages = gplResult.result.length;
			for( var i in gplResult.result ) { 
				
				sourcePackages[i] = { 
						name : gplResult.result[i],
						processed : false,
						data : {}
				};
				
			} //for
			
			logger.log( numberOfSourcePackages + " Source packages stored, goto processing", LOG_TYPE_DEBUG);
			logger.log( "", LOG_TYPE_TRACE);
			
			//When all information is stored, process sources, keep this in its own function!  (even thought is it just a loop)
			processFrom = 0;
			processSourcePackagesList( );
        }
        else {
			//There was an error, log
            logger.log( "There was an error in 'getPackageList'.'package_list'.", LOG_TYPE_ERR );
            logger.log( gplErr.toString() , LOG_TYPE_ERR);
        }
    });
	
};


//Process source packages in steps of [stepSize]
function processSourcePackagesList( ) {
	processTo = processFrom+stepSize-1;
	if( processTo > numberOfSourcePackages-1 ) {
		processTo = numberOfSourcePackages-1;
	}
	
	logger.log( "Processing source packages " + processFrom + " / " + processTo, LOG_TYPE_INFO);

    var from = processFrom;
    var to = processTo;
	for( var i = processFrom ; i<= processTo ; i++ ) {	//Dont log inside this for, processSourcePackage has a few async callbacks
		getPackages( i );
	}
	
}


//Loads the source and target packages for [i] 
function getPackages( i ) {
	logger.log( "Processing source package " + i, LOG_TYPE_INFO);
	
	var pckgName = sourcePackages[i].name;
	
	 //+ " / " + pckgName
	
	//Load the source package
    ckan_dpf.action('package_show', { id : pckgName  }, function( psp_source_Err, psp_source_Result) {
        //Check for error
		if( !psp_source_Err && psp_source_Result.result.type == "dataset") {
            sourcePackages[i].data = psp_source_Result.result;
			
			logger.log( "Got source package " + i + ", get target package " + i, LOG_TYPE_DEBUG);

			//Load the target package
			ckan_donl.action('package_search', {"rows":1,"q":"identifier:" + sourcePackages[i].data.id,"fq":"+organization:" + dpfOrganization + " +type:dataset"}, function( psp_target_Err, psp_target_Result) {
				//Check for error
				if( !psp_target_Err && psp_target_Result.success) {
					if (psp_target_Result.result.count == 1) {
						var dataset = psp_target_Result.result.results[0];

						//Package exists in Target, UPDATE
						logger.log("Got target package " + i + ", UPDATE", LOG_TYPE_DEBUG);

						//Store target package
						targetPackages[i] = {
							name: pckgName,
							processed: false,
							data: dataset
						};

						createSendPackage(i, 'update');
					} else {
						//Package doesn't exists in Target, CREATE
						logger.log( "No target package, CREATE " + i , LOG_TYPE_DEBUG);

						//Store target package
						targetPackages[i] = {
							name : pckgName,
							processed : false,
							data : {}
						};

						createSendPackage(i, 'create');
					}
				}
				else {
					//There was an error, log
					logger.log( "There was a target error in 'getPackages'.'package_show'.",LOG_TYPE_ERR );
					logger.log( "For package : " + pckgName , LOG_TYPE_ERR);
					logger.log( psp_target_Err.toString() , LOG_TYPE_ERR);

					if (psp_target_Err.toString().indexOf("ECONNRESET") > -1) {
					    logger.log("Retry due in 1 sec...", LOG_TYPE_INFO);
					    sleep(1000);
					    getPackages(i);
                    }
				}   
				
			}); //ckan_donl package_show
			
        }
        else {
			if (psp_source_Err) {
				//There was an error, log
				console.log("There was an source error in 'getPackages'.'package_show'.", LOG_TYPE_ERR);
				console.log(psp_source_Err.toString(), LOG_TYPE_ERR);

                if (psp_source_Err.toString().indexOf("ECONNRESET") > -1) {
                    logger.log("Retry due in 1 sec...", LOG_TYPE_INFO);
                    sleep(1000);
                    getPackages(i);
                }
			} else {
				// No error, but package is no dataset, skip
				console.log("Package " + i + " (" + pckgName + ") in not of type 'dataset', but '" + psp_source_Result.result.type + "', skipping...");

				logger.log( "Package is no dataset, NONE " + i , LOG_TYPE_DEBUG);

				//Store target package
				targetPackages[i] = {
					name : pckgName,
					processed : true,
					data : {}
				};

				createSendPackage(i, 'none');
			}
        }   
        
    }); //ckan_dpf package_show
	
	
}








/*
 *  CREATION OF THE CREATE/UPDATE PACKAGES
 *
 */

//Create an object to update/create 
function createSendPackage(i, action) {
	
	
	//UPDATE
	if( action == 'update') {	
		var pckg = createUpdatePackage( i )
		if( pckg.action == 'update' ) {
			sendPackages[i] = {
				action	: 'update',
				send 	: false,
				data 	: pckg.data,
				md_uri  : pckg.md_uri
			};
		}
		else {
			//No need for update
			sendPackages[i] = {
				action	: 'none',
				send 	: false,
				data 	: {},
				md_uri  : pckg.md_uri
			};
		}
	} //if update
	else if ( action == 'none') {
		sendPackages[i] = {
			action	: 'none',
			send 	: false,
			data 	: {},
		};
	}// else if 'none'
	
	//CREATE
	else {	
		var pckg = createNewPackage( i )
		sendPackages[i] = {
			action	: 'create',
			send 	: false,
			data 	: pckg.data,
			md_uri  : pckg.md_uri
		};
	}  //else : if update
	
	
	//Mark packages as processed
	targetPackages[i].processed = true;
	sourcePackages[i].processed = true;
	
	logger.log( "Created send package " + i , LOG_TYPE_DEBUG);
	
	checkIfProcessed( );
	
}


//Returns a create package
function createNewPackage( i ) {
    logger.log( "Create INSERT package " + i , LOG_TYPE_DEBUG);
    
	
	//The source package to use for the CREATE packge
	var pckg = sourcePackages[i].data;
	

    //Do mappings
    var strMaintainer 	= doPublisherMapping( pckg );
    var strTheme      	= doThemeMapping( pckg );
    var lang          		= doLanguageMapping( pckg );
    var license       	= doLicenseMapping(pckg );
    
	
    //Create timestamp dd-mm-yyyy
    var d = new Date();
    var ts = ("0" + d.getDate()).slice(-2) + "-" + ("0"+(d.getMonth()+1)).slice(-2) + "-" + d.getFullYear() ;
        
    //Add a orginal_url property, this is used in the CKAN templates (url seems to change to CKAN's own domain...)        
    var resources = pckg.resources
    for( var resIndex in resources ) {
        resources[resIndex].orginal_url = resources[resIndex].url;   
    }
    
    
    //Create a new package
    var targetpackage = {
        
        //Hardcoded or lookup/mapping
        modified         	: ts,        
        language         	: lang,
        dataset_status		: 'http://data.overheid.nl/status/beschikbaar',
        maintainer       	: strMaintainer,
        //maintainer_email	: 'ckan@dataplatform.nl',        
        maintainer_email	: pckg.publisher_email,        
        owner_org        	: organization_dataplatform.id, //Set the data owner (Catalogus in DONL)
        theme            	: strTheme,
        theme_facet      	: strTheme,  //Not set?
        accessibility 		: 'http://data.overheid.nl/acessrights/publiek',
        
        //Values from source
        name            	: pckg.name,
        title            		: pckg.title,
        author           	: pckg.author,
        author_email   	: pckg.author_email,
        
		
		license_id       	: license,
		license_title		: ( license == "Licentie onbekend" ? "notspecified" : license ),
		
        notes            	: pckg.notes,
        url              		: pckg.url,
        version          	: pckg.version,
        state            		: pckg.state,
        type             		: pckg.type,
        
        
//		maintainer
		
        //Link met dataplatform
        md_uri 		: 'https://www.dataplatform.nl/dataset/' + pckg.id,  //Catalogusreferentie in DONL
        identifier 	: pckg.id, //Catalogus-identifier in DONL
        
        
        
        
        //These are ARRAYs :
        resources                	: resources,		
        tags                     	: pckg.tags,
        relationships_as_object  	: pckg.relationships_as_object,
        relationships_as_subject 	: pckg.relationships_as_subject,
        
    };
    
    
    
    
    return {
		action : 'create',
		data : targetpackage,
		md_uri : targetpackage.md_uri
	};
	
}       


//Returns an update package
function createUpdatePackage( i ) {
    logger.log( "Create UPDATE package " + i , LOG_TYPE_DEBUG);
    
	//The source & target package to use for the UPDATE packge
	var target_pckg	= targetPackages[i].data;
	var source_pckg	= sourcePackages[i].data;
	
    var new_pckg = {};
    
    //Mapping Values
    var strMaintainer 	= doPublisherMapping( source_pckg );
    var strTheme      	= doThemeMapping( source_pckg );
    var lang          	= doLanguageMapping( source_pckg );
    var license       	= doLicenseMapping( source_pckg );
    
    //Update mapping values?
    if( ( target_pckg.maintainer   	!= strMaintainer	) ) { new_pckg.maintainer   = strMaintainer;	}
    if( ( target_pckg.theme        	!= strTheme       	) ) { new_pckg.theme        = strTheme ;      	}
    if( ( target_pckg.theme_facet  	!= strTheme       	) ) { new_pckg.theme_facet	= strTheme ;  		} 
    if( ( target_pckg.language     	!= lang           	) ) { new_pckg.language     = lang ;          	}
    if( ( target_pckg.license   	!= license        	) ) { 
		new_pckg.license   	   = license ;       	
		new_pckg.license_title = ( license == "Licentie onbekend" ? "notspecified" : license );
		new_pckg.license_id    = license ;
	}
   
    //Update text values?
    if( ( source_pckg.email   	!= target_pckg.email  	) ) { new_pckg.email  	= source_pckg.email ;  	}
    //if( ( source_pckg.name   	!= target_pckg.name  	) ) { new_pckg.name  	= source_pckg.name ;   	} // Naam niet updaten, kan aangepast zijn vanwege noodzaak tot uniciteit in CKAN
    if( ( source_pckg.title     != target_pckg.title    ) ) { new_pckg.title   	= source_pckg.title ;   }
    if( ( source_pckg.email  	!= target_pckg.email  	) ) { new_pckg.email 	= source_pckg.email ;  	}
    if( ( source_pckg.notes   	!= target_pckg.notes  && source_pckg.notes != ''	) ) { new_pckg.notes  	= source_pckg.notes ;  	}
    if( ( source_pckg.url       != target_pckg.url      ) ) { new_pckg.url      	= source_pckg.url ; }
    if( ( source_pckg.version	!= target_pckg.version	) ) { new_pckg.version	= source_pckg.version ;	}
    if( ( source_pckg.state   	!= target_pckg.state    ) ) { new_pckg.state   	= source_pckg.state ;   }
    if( ( source_pckg.type    	!= target_pckg.type     ) ) { new_pckg.type   	= source_pckg.type ;    }
    
    if( ( source_pckg.publisher_email != target_pckg.maintainer_email     ) ) { new_pckg.maintainer_email   	= source_pckg.publisher_email ;    }
    
	//Special fix for author, if we pass "" to the API, it ends up as null, so extra check to prevent needless updates
	if( ( source_pckg.author != target_pckg.author	) && ( source_pckg.author != "" && target_pckg.author != null ) ) { 
		new_pckg.author	= source_pckg.author ;
	}

	//Onetime fix:
	new_pckg.accessibility = 'http://data.overheid.nl/acessrights/publiek';
	
	//Fix for orginal_url property
	// var resources = source_pckg.resources
    // for( var resIndex in resources ) {
    //     resources[resIndex].orginal_url = resources[resIndex].url;   
    // }
    // new_pckg.resources = resources;
    
	
	//new_pckg.maintainer = strMaintainer;
	
    //Values from Arrays (Tags, Resources) TODO
    
	//tmp patch!
	//new_pckg.language     = lang;
	//new_pckg.dataset_status = 'http://data.overheid.nl/status/beschikbaar';
    //new_pckg.maintainer = strMaintainer;
	//new_pckg.owner_org  = organization_dataplatform.id;
    
    
    //Basicly, has anything been added to new_pckg ?  (google this!)
    if( !(Object.keys(new_pckg).length === 0 && new_pckg.constructor === Object)  ) {
        
        //Yes, there have been, update updated timestamp
        var d = new Date();
        var ts = ("0" + d.getDate()).slice(-2) + "-" + ("0"+(d.getMonth()+1)).slice(-2) + "-" + d.getFullYear() ;
        
		new_pckg.modified = ts;
		
		new_pckg.extras = [];
		
        //Set the ID
        new_pckg.id = target_pckg.id;
           
		//console.log(  source_pckg.title );   
		//console.log( new_pckg );
		
	//console.log( "S:" +  source_pckg.author );	//	== ""
	//console.log( "T:" +  target_pckg.author );   //  == null
	
        //Store the package
        return {
				action : 'update',
				data : new_pckg,
				md_uri : 'https://www.dataplatform.nl/dataset/' + source_pckg.id,  //Catalogusreferentie in DONL
		}
        
    } 
	else {
        //No need for update
        return  {
				action : 'none',
				data : {}
		}
        
    }
    
}


//Check if the packages have been processed, (if source and target are processed, sendPackges should exists [createSendPackage MUST take care of that] ) 
function checkIfProcessed( ) {
	var processed = true;
	
	logger.log( "Check if processed " + processFrom + " / " + processTo, LOG_TYPE_TRACE);
	
	for( var i = processFrom ; i<= processTo ; i++ ) {	//Dont log inside this for, processSourcePackage has a few async callbacks
		
		if( ! sourcePackages[i] ) {
			logger.log( "Check if processed " + i + " failed(1)", LOG_TYPE_DEBUG);
			processed = false;
			break;
		}
		if( ! targetPackages[i] ) {
			logger.log( "Check if processed " + i + " failed(2)", LOG_TYPE_DEBUG);
			processed = false;
			break;
		}
		
		if( ! sourcePackages[i].processed ) {
			logger.log( "Check if processed " + i + " failed(3)", LOG_TYPE_DEBUG);
			processed = false;
			break;
		}
		
		if( ! targetPackages[i].processed ) { 
			logger.log( "Check if processed " + i + " failed(4)", LOG_TYPE_DEBUG);
			processed = false;
			break;
		}
		
		logger.log( "Check if processed " + i + " passed", LOG_TYPE_DEBUG);
	}
	
	if( processed ) {
		processFrom += stepSize;
		
		//Next batch?
		if( processFrom < numberOfSourcePackages ) {
			logger.log( "Batch " + (processFrom-stepSize) + " / " + processTo + " processed, next batch " , LOG_TYPE_INFO);
			logger.log( "", LOG_TYPE_INFO);
			
			sleep(sleepTime);
			
			
			processSourcePackagesList();
		}
		else{
			logger.log( "All batches processed and sent. ", LOG_TYPE_INFO);
			logger.log( "" , LOG_TYPE_INFO);
			
			sendFrom = 0;
			sendPackagesToApi( ) ;
		}
	}
	else {
		logger.log( "Not all processed, continue", LOG_TYPE_TRACE);
	}
	
	
}







/*
 *  SENDING TO CKAN
 *
 */


//Send source packages in steps of [stepSize]
function sendPackagesToApi( ) {
	sendTo = sendFrom+stepSize-1;
	if( sendTo > numberOfSourcePackages-1 ) {
		sendTo = numberOfSourcePackages-1;
	}
	
	logger.log( "Sending packages " + sendFrom + " / " + sendTo, LOG_TYPE_INFO);
	
	var from = sendFrom;
	var to   = sendTo;
	for( var i = from ; i<= to ; i++ ) {	//Dont log inside this for, processSourcePackage has a few async callbacks
		sendPackage( i );
	}
	
}


//Update of create a package, this needs to be it's own function because now i has the correct value inside the create/patch callbacks
function sendPackage( i ) { 

	//Package exists, No update needed
	if( sendPackages[i].action == 'none' ) {
		//Mark package as send (not reallly send, but we need this to be true for the send check, processed would be a better property name)
		sendPackages[i].send = true;
		
		logger.log("Package "  + i+ " does not require an action.", LOG_TYPE_ERR);
		
		checkIfSend( );
	}
	
	
	//Package exists, update needed
	else if( sendPackages[i].action == 'update' ) {
	    
	    
		    
        ckan_donl.action('package_patch', sendPackages[i].data, function(err, result) {
            
			//Check for errors in result
            if( !err ) {
                logger.log("Package "  + i+ " has been updated.", LOG_TYPE_INFO);
            }
            else {
                logger.log( "There was an error in 'sendPackage'.'package_patch'.", LOG_TYPE_ERR );
                logger.log( err.toString() , LOG_TYPE_ERR);
                //console.log( sendPackages[i].data );

                if (err.toString().indexOf('409') > -1) { // Validation error
                    logger.logValidationErrors(result, sendPackages[i].md_uri);
                } else {
					if (err.toString().indexOf("ECONNRESET") > -1) {
					    logger.log("Retry due in 1 sec...", LOG_TYPE_INFO);
					    sleep(1000);
					    sendPackage(i);
                    }
                }
            }
			
			//Mark package as send
			sendPackages[i].send = true;
			
			checkIfSend( );
			
        });
	}
	
	//Package doesn't exists, create
	else {
		ckan_donl.action('package_create', sendPackages[i].data, function(err, result) {
            if( !err ) {
                logger.log("Package "  + i + " has been created.", LOG_TYPE_INFO);
            }
            else if (err.toString().indexOf("De opgegeven titel is reeds in gebruik.") > -1) { // name duplicate
				var name = sendPackages[i].data.name;
				sendPackages[i].data.name = name + "-" + sendPackages[i].data.identifier.toLowerCase()

				logger.log("Package " + i + " name already in use (" + name + "). Add identifier (new name: " + sendPackages[i].data.name + ") and try again...", LOG_TYPE_INFO);

				ckan_donl.action('package_create', sendPackages[i].data, function(err, result) {
					if( !err ) {
						logger.log("Package "  + i + " has been created.", LOG_TYPE_INFO);
					}
					else {
						logger.log( "There was an error in 'sendPackage'.'package_create'.", LOG_TYPE_ERR );
						logger.log( err.toString() , LOG_TYPE_ERR);

                        if (err.toString().indexOf('409') > -1) { // Validation error
    						logger.logValidationErrors(result, sendPackages[i].md_uri);
                        } else {
                            if (err.toString().indexOf("ECONNRESET") > -1) {
                                logger.log("Retry due in 1 sec...", LOG_TYPE_INFO);
                                sleep(1000);
                                sendPackage(i);
                            }
                        }
					}

					//Mark package as send
					sendPackages[i].send = true;

					checkIfSend( );
				});
			}
			else {
                logger.log( "There was an error in 'sendPackage'.'package_create'.", LOG_TYPE_ERR );
                logger.log( err.toString() , LOG_TYPE_ERR);

                if (err.toString().indexOf('409') > -1) { // Validation error
                    logger.logValidationErrors(result, sendPackages[i].md_uri);
                } else {
                    if (err.toString().indexOf("ECONNRESET") > -1) {
                        logger.log("Retry due in 1 sec...", LOG_TYPE_INFO);
                        sleep(1000);
                        sendPackage(i);
                    }
                }
            }
            
			//Mark package as send
			sendPackages[i].send = true;
			
			checkIfSend( );
        });
	}
	
	
	
	
	
}

//Performs a check if all package in the current batch have been send to CKAN
function checkIfSend( ) {
	var allsend = true;
	
	logger.log( "Check if send " + sendFrom + " / " + sendTo, LOG_TYPE_TRACE);
	
	for( var i = sendFrom ; i<= sendTo ; i++ ) {	//Dont log inside this for, processSourcePackage has a few async callbacks
		
		if( ! sendPackages[i].send  ) {
			logger.log( "Check if send " + i + " failed", LOG_TYPE_DEBUG);
			allsend = false;
			break;
		}

		logger.log( "Check if send " + i + " passed", LOG_TYPE_DEBUG);
	}
	
	if( allsend ) {
		sendFrom += stepSize;
		
		//Next batch?
		if( sendFrom < numberOfSourcePackages ) {
			logger.log( "Batch " + (sendFrom-stepSize) + " / " + sendTo + " send, next batch " , LOG_TYPE_INFO);
			logger.log( "", LOG_TYPE_INFO);
			
			sleep(sleepTime);
			
			sendPackagesToApi();
		}
		else{
			
			logger.log( "All Batches send! ", LOG_TYPE_INFO);
			logger.log( "" , LOG_TYPE_INFO);
			
			//logger.finalize();
			
			getPackagesForDelete();
		}
	}
	else {
		logger.log( "Not all send, continue", LOG_TYPE_TRACE);
	}
	
	
	
}






/*
 *  REMOVE ITEMS IN TARGET THAT ARE NO LONGER AVAILABLE IN SOURCE
 *
 */

function getPackagesForDelete() {
	numberOfDeletePackages = 0;
	logger.log( "Getting target package list for delete", LOG_TYPE_INFO);

	//Search for dataplatform packages
    ckan_donl.action('package_search', { q : 'organization:' + dpfOrganization, fq: 'type:dataset', rows: 0 }, function( gpdErr, gpdResult) {
		//Check for errors
        if( !gpdErr ) {
			
			logger.log("CKAN DONL Found " + gpdResult.result.count + " dataplatform packages", LOG_TYPE_DEBUG)

			var totalRows = gpdResult.result.count;
			
			//Seach again, but this time, get ALL dataplatform packages
			ckan_donl.action('package_search', { q : 'organization:' + dpfOrganization, fq: 'type:dataset', rows : totalRows }, function( gpdErr2, gpdResult2) {
					
				if( !gpdErr2 ) {
					
					//No errors, store information
					numberOfDeletePackages = totalRows;
					for( var i in gpdResult2.result.results ) { 
						//console.log( gpdResult2.result.results[i].name );
						deletePackages[i] = {
								name : gpdResult2.result.results[i].name,
								donlId : gpdResult2.result.results[i].id,
								dpfId : gpdResult2.result.results[i].identifier,
								processed : false,
								send : false,
								data : {}
						};
					} //for
					
					logger.log( numberOfDeletePackages + " Source packages for delete stored, goto processing", LOG_TYPE_DEBUG);
					logger.log( "", LOG_TYPE_TRACE);
					
					
					//When all information is stored, process sources, keep this in its own function!  (even though it is just a loop)
					processFrom = 0;
					processDeletePackagesList( );
				}
				else {
					//There was an error, log
					logger.log( "There was an error in 'getPackagesForDelete'.'package_search(2)'.", LOG_TYPE_ERR );
					logger.log( gpdErr2.toString() , LOG_TYPE_ERR);
					
				}
				
			});
        }
        else {
			//There was an error, log
            logger.log( "There was an error in 'getPackagesForDelete'.'package_search(1)'.", LOG_TYPE_ERR );
            logger.log( gpdErr.toString() , LOG_TYPE_ERR);
			
        }
    });
	
}

function processDeletePackagesList() {
	processTo = processFrom+stepSize-1;
	if( processTo > numberOfDeletePackages-1 ) {
		processTo = numberOfDeletePackages-1;
	}
	
	logger.log( "Processing source packages for delete " + processFrom + " / " + processTo, LOG_TYPE_INFO);
    
    var from  = processFrom;
    var to = processTo;
	for( var i = from ; i<= to ; i++ ) {	//Dont log inside this for, processSourcePackage has a few async callbacks
		getDeletePackage( i );
	}
	
}

function getDeletePackage( i ) {

	logger.log( "Processing source package for delete " + i, LOG_TYPE_INFO);

	//Load the target package
    ckan_donl.action('package_show', { id : deletePackages[i].donlId  }, function( psp_source_Err, psp_source_Result) {
        //Check for error
		if( !psp_source_Err ) {
            deletePackages[i].data = psp_source_Result.result;
			
			logger.log( "Got target package for delete " + i + ", get source package for delete " + i, LOG_TYPE_INFO);
			
			
			//Load the target package
			ckan_dpf.action('package_show', { id : deletePackages[i].dpfId  }, function( psd_target_Err, psd_target_Result) {
				//Check for error
				if( !psd_target_Err ) {
					//Package exists, do nothing
					
					
					logger.log( "DONL Package '" + psp_source_Result.result.name + "' found in DPF '" + psd_target_Result.result.name + "', No delete " + i, LOG_TYPE_INFO);
					
					deletePackages[i].action = 'none';
					deletePackages[i].processed = true;
					//console.log( "Call 'checkIfDeleteProcessed' from 'getDeletePackage( "+i+" ) 1'" );
					checkIfDeleteProcessed( );
				}
				else if( psd_target_Err.toString().indexOf('"Niet gevonden"') > -1 || psd_target_Err.toString().indexOf("name_or_id") > -1) {
					//Package doesn't exists, delete
					
					logger.log( "DONL Package '" + psp_source_Result.result.name + "' not found in DPF , package delete" + i, LOG_TYPE_INFO);
					
					
					deletePackages[i].action = 'delete';
					deletePackages[i].processed = true;
					
					//console.log("DELETE!");
					//console.log(deletePackages[i].name);
					//console.log( "Call 'checkIfDeletePro cessed' from 'getDeletePackage( "+i+" ) 2'" );
					checkIfDeleteProcessed( );
				}
				else if( psd_target_Err.toString().indexOf('Toestemming geweigerd') > -1 ) { 
					//Package exists, do nothing
					logger.log( "DONL Package '" + psp_source_Result.result.name + "', access denied in DPF , no delete" + i, LOG_TYPE_INFO);
					
					deletePackages[i].action = 'delete';
					deletePackages[i].processed = true;
					
					//console.log("ACCESS DENIED! : " + pckgName);
					
					//console.log( "Call 'checkIfDeleteProcessed' from 'getDeletePackage( "+i+" ) 3'" );
					checkIfDeleteProcessed( );
				}
				//There was an error, log
				else {
					logger.log( "There was a source error in 'getDeletePackages'.'package_show'(1).",LOG_TYPE_ERR );
					logger.log( "pckgName : " + deletePackages[i].name, LOG_TYPE_ERR);
					logger.log( psd_target_Err.toString() , LOG_TYPE_ERR);

					if (psp_target_Err.toString().indexOf("ECONNRESET") > -1) {
					    logger.log("Retry due in 1 sec...", LOG_TYPE_INFO);
					    sleep(1000);
					    getDeletePackage(i);
                    }

				}   
				
			}); //ckan_donl package_show
			
        }
        else {
			//There was an error, log
            logger.log( "There was a target error in 'getDeletePackages'.'package_show'(2).",LOG_TYPE_ERR );
            logger.log( psp_source_Err.toString(),LOG_TYPE_ERR );

            if (psp_source_Err.toString().indexOf("ECONNRESET") > -1) {
                logger.log("Retry due in 1 sec...", LOG_TYPE_INFO);
                sleep(1000);
                getDeletePackage(i);
            }

        }   
        
    }); //ckan_dpf package_show
	
	
}

function checkIfDeleteProcessed( ) {
	var processed = true;
	
	logger.log( "Check if delete processed " + processFrom + " / " + processTo, LOG_TYPE_TRACE);
	
	for( var i = processFrom ; i<= processTo ; i++ ) {	//Dont log inside this for, processSourcePackage has a few async callbacks
		
		if( ! deletePackages[i] ) {
			logger.log( "Check if delete processed " + i + " failed(1)", LOG_TYPE_DEBUG);
			processed = false;
			break;
		}
		
		if( ! deletePackages[i].processed ) {
			logger.log( "Check if delete processed " + i + " failed(2)", LOG_TYPE_DEBUG);
			processed = false;
			break;
		}
		
		logger.log( "Check if delete processed " + i + " passed", LOG_TYPE_DEBUG);
	}
	
	if( processed ) {
		processFrom += stepSize;
		
		//Next batch?
		if( processFrom < numberOfDeletePackages ) {
			logger.log( "Batch " + (processFrom-stepSize) + " / " + processTo + " for delete processed, next batch " , LOG_TYPE_INFO);
			logger.log( "", LOG_TYPE_INFO);
			
			sleep(sleepTime);
			
			console.log("call 'processDeletePackagesList' from 'checkIfDeleteProcessed'");
			processDeletePackagesList();
		}
		else{
			logger.log( "All Batches for delete processed, send! ", LOG_TYPE_INFO);
			logger.log( "" , LOG_TYPE_INFO);
			
			sendFromDel = 0;
			console.log("call 'sendDeletePackages' from 'checkIfDeleteProcessed'");
			sendDeletePackages( ) ;
		}
	}
	else {
		logger.log( "Not all for delete processed, continue", LOG_TYPE_TRACE);
	}
	
	
}

function sendDeletePackages() {
	var guid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    var r = Math.random()*16|0, v = c == 'x' ? r : (r&0x3|0x8);
        return v.toString(16);
    });

	//console.log(">>sendDeletePackages() :: " + guid);
	
	sendToDel = sendFromDel+stepSize-1;
	if( sendToDel > numberOfDeletePackages-1 ) {
		sendToDel = numberOfDeletePackages-1;
	}
	
	//logger.log( "Processing send packages for delete " + sendFromDel + " / " + sendToDel, LOG_TYPE_INFO);
    
    //Store from/to in other variables (sendFromDel & sendToDel can be modified outside this function!)
    var from  = sendFromDel;
    var to    = sendToDel;
	for( var i = from ; i<= to ; i++ ) {	//Dont log inside this for, processSourcePackage has a few async callbacks
		//console.log( "Call 'deletePackage('" + i + ")' from 'sendDeletePackages' :: " + guid );
		deletePackage( i );
	}
	
}

function deletePackage( i ) {
	//console.log( ">>deletePackage(" + i + ")" );
	
	
	if( !deletePackages[i].send && deletePackages[i].action == 'delete' ) {
		
		logger.log( "CKAN DONL dataset purge " + i , LOG_TYPE_INFO);
		
		ckan_donl_admin.action('dataset_purge', { id : deletePackages[i].data.id }, function(err, result) {
            if( !err ) {
                logger.log("Package "  + i + " has been deleted.", LOG_TYPE_ERR);
            }
            else {
                logger.log( "There was an error in 'deletePackage("+i+")'.'dataset_purge'.", LOG_TYPE_ERR );
                logger.log( err.toString() , LOG_TYPE_ERR);

                if (err.toString().indexOf("ECONNRESET") > -1) {
                    logger.log("Retry due in 1 sec...", LOG_TYPE_INFO);
                    sleep(1000);
                    deletePackage(i);
                }
            }
            deletePackages[i].send = true;
    	 //   console.log( "Call 'checkIfDeleteSend' from 'deletePackage(" + i + ")'" );
    	    checkIfDeleteSend();
        });
	}
	else {
	    deletePackages[i].send = true;
	   // console.log( "Call 'checkIfDeleteSend' from 'deletePackage(" + i + ")'" );
	    checkIfDeleteSend();
	}
	
}

//Check if the packages have been processed, (if source and target are processed, sendPackages should exists [createSendPackage MUST take care of that] )
function checkIfDeleteSend( ) {
	//console.log(">>checkIfDeleteSend()");
	
	var send = true;
	
	logger.log( "Check if delete send " + sendFromDel + " / " + sendToDel, LOG_TYPE_TRACE);
	for( var i = sendFromDel ; i<= sendToDel ; i++ ) {
		
		if( ! deletePackages[i] ) {
			logger.log( "Check if delete send " + i + " failed(1)", LOG_TYPE_DEBUG);
			send = false;
			break;
		}
		
		if( ! deletePackages[i].send ) {
			logger.log( "Check if delete send " + i + " failed(2)", LOG_TYPE_DEBUG);
			send = false;
			break;
		}
		
		logger.log( "Check if delete send " + i + " passed", LOG_TYPE_DEBUG);
	}
	
	if( send ) {
		sendFromDel += stepSize;
		
		//Next batch?
		if( sendFromDel < numberOfDeletePackages ) {
			logger.log( "Batch " + (sendFromDel-stepSize) + " / " + sendToDel + " for delete send done, next batch " , LOG_TYPE_INFO);
			logger.log( "", LOG_TYPE_INFO);
			
			sleep(sleepTime);
			
			//console.log("call 'sendDeletePackages' from 'checkIfDeleteSend'");
			sendDeletePackages();
		}
		else{
			logger.log( "All Batches for delete have been send! ", LOG_TYPE_INFO);
			logger.log( "" , LOG_TYPE_INFO);
			
			logger.finalize();
			
			//sendFrom = 0;
			//sendPackagesToApi( ) ;
		}
	}
	else {
		logger.log( "Not all for delete send, continue", LOG_TYPE_TRACE);
	}
	
	
}







//-----------------------------------------------------------------------------
//
//  MAPPING FUNCTIONS
//
//-----------------------------------------------------------------------------

//if (typeof pckg.license_id == 'undefined' || pckg.license_id === null ) {
 
//Publisher / Organisatie mapping
function doPublisherMapping( pckg ) {
    
    //Map Publisher (Organisatie)
    if (typeof pckg.publisher_uri == 'undefined') {
        logger.appendToMappingLog( "publisher", pckg.publisher_uri, "pckg.publisher_uri == 'undefined'", pckg.id );
        return 'Onbekend';
    }
	else if (pckg.publisher_uri === null ) {
        logger.appendToMappingLog( "publisher", pckg.publisher_uri, "pckg.publisher_uri == null", pckg.id );
        return 'Onbekend';
    }
	else if (pckg.publisher_uri == '' ) {
        logger.appendToMappingLog( "publisher", pckg.publisher_uri, "pckg.publisher_uri == ''", pckg.id );
        return 'Onbekend';
    }
    else {
		if( pckg.publisher_uri.toLowerCase() == 'onbekend' ) {
			logger.appendToMappingLog( "publisher", pckg.publisher_uri, "pckg.publisher_uri == 'onbekend'", pckg.id );
			return 'Onbekend';
		}
        for( var i in dataEigenaarMapping ) {
            if( dataEigenaarMapping[i].name.toLowerCase() == pckg.publisher_uri.toLowerCase() ) {
                return dataEigenaarMapping[i].field_uri_organisatie;
            }
        }
    }
    
    logger.appendToMappingLog( "publisher", pckg.publisher_uri, "Value not found in mapping", pckg.id );
    return pckg.publisher_uri;
}


//Thema mapping
function doThemeMapping( pckg ) {
    
    //if( pckg.name == 'aantal-leerlingen-voortgezet-onderwijs' ) {
	//	console.log( pckg );
	//	console.log( typeof pckg.theme == 'undefined' );
    //}
    
    //Map Theme
    if (typeof pckg.theme == 'undefined') {
        logger.appendToMappingLog( "theme", pckg.theme, "pckg.theme == 'undefined'", pckg.id )
        return 'onbekend';
    }
    else {
		
        for( var i in dataThemaMapping ) {
			
			//if( pckg.name == 'aantal-leerlingen-voortgezet-onderwijs' ) {
			//    console.log( ">>" + pckg.theme.toLowerCase() + "  /  " + dataThemaMapping[i].name.toLowerCase() + " ==  " +  dataThemaMapping[i].field_uri_thema);
		    //}
		    
            if( dataThemaMapping[i].name.toLowerCase() == pckg.theme.toLowerCase() ) {
				
				//console.log("Theme => " + dataThemaMapping[i].field_uri_thema );
				
                return dataThemaMapping[i].field_uri_thema;
            }
        }
    }
    
    logger.appendToMappingLog( "theme", pckg.theme, "Value not found in mapping", pckg.id );
    return pckg.theme;
}


//License mapping
function doLicenseMapping( pckg ) {
    if (typeof pckg.license_id == 'undefined' || pckg.license_id === null ) {
        logger.appendToMappingLog( "license", pckg.license_id, "pckg.license_id == 'undefined' or === null", pckg.id )
        return 'Licentie onbekend';
    }
	else if (typeof pckg.license_id == 'onbekend'  ) {
        logger.appendToMappingLog( "license", pckg.license_id, "pckg.license_id == 'onbekend' or === null", pckg.id )
        return 'Licentie onbekend';
    }
    else {
        if( pckg.license_id.toLowerCase()  == 'other-pd' ) { return 'publiek-domein'; }
        if( pckg.license_id.toLowerCase()  == 'cc-zero' )  { return 'cc-0'; }
    }
    
    logger.appendToMappingLog( "license", pckg.license_id, "Value not found in mapping", pckg.id );
    return pckg.license_id;
}


//Language mapping
function doLanguageMapping( pckg ) {
    
    
    if( pckg.language == "" ) {
        return 'nl-NL';
    }
    
    if (typeof pckg.language == 'undefined') {
        logger.appendToMappingLog( "language", pckg.language, "pckg.language == 'undefined'", pckg.id )
        return 'nl-NL';
    }
	
    else {
        if( pckg.language.toLowerCase()  == 'ned' ) { return 'nl-NL'; }
        if( pckg.language.toLowerCase()  == 'nl-nl' ) { return 'nl-NL'; }
        if( pckg.language.toLowerCase()  == 'nl'  )  { return 'nl-NL'; }
        if( pckg.language.toLowerCase()  == 'nederlands'  ) { return 'nl-NL'; }
    }
    
    logger.appendToMappingLog( "language", pckg.language, "Value not found in mapping", pckg.id );
    return pckg.language;
}



//Load the data for the different mappings
function loadMappings() {
    
    //First get the 'mapping_organisatie' mapping from Drupal Taxonomy
    request.get({ url : "https://data.overheid.nl/service/waardelijsten/mapping_organisatie" }, 
        function(error, response, body) {
            if( !error ) {
                //Set mapping data
                dataEigenaarMapping = JSON.parse(body);
                
                //Next, get the 'thema_mapping' mapping from Druapl Taxonomy
                request.get({ url : "https://data.overheid.nl/service/waardelijsten/mapping_thema" }, 
                    function(error, response, body) {
                        
                        if( !error ) {
                            
                            //Set mapping data
                            dataThemaMapping = JSON.parse(body);
                            
                            ckan_donl.action('organization_show', { id : 'dataplatform' }, function(err, result) {
                            //Check for errors in result
                            if( !err ) {
                                
                                //Set data
                                organization_dataplatform = result.result;

								var dpfUsers = organization_dataplatform.users.filter(function(item) { return item.fullname == "dataplatform" });
								if (dpfUsers !== undefined) {
									var dpfUserId = dpfUsers[0].id;
									ckan_donl.action('user_show', { id : dpfUserId }, function(err, result) {
										if (!err && result.result.email !== undefined) {
											if (sendMailsToCkanUser) {
												validatieMailConfig.to = validatieMailConfig.to + "," + result.result.email;
												console.log("dataplatform e-mail: ", result.result.email);
											} else {
												console.log("dataplatform e-mail: ", result.result.email + " (maar we gebruiken deze niet omdat send_mails_to_ckan_user in context op false staat, fallback naar " + validatieMailConfig.to + ")");
											}
										}
									});
								}
                                
                                //We don't need these
                                delete organization_dataplatform.users;
                                delete organization_dataplatform.extras;
                                delete organization_dataplatform.groups;
                                delete organization_dataplatform.tags;
                                
                                getPackageList();
                                
                            }
                            else {
                                console.log("There was an error in 'organization_show'. This is not fatal, but this package will be skipped. Perhaps these details can help you: ");
                                console.log(err);
                            }
                            });
                        }
                        else {
                            console.log("There was an error loading 'thema_mapping'. This is fatal I'm afraid. Perhaps these details can help you: ");
                            console.log(error);
                        }
                    }
                );
            }
            else {
                console.log("There was an error loading 'mapping_organisatie'. This is fatal I'm afraid. Perhaps these details can help you: ");
                console.log(error);
            }
        }
    );
}



//-----------------------------------------------------------------------------
//
//  LET'S GET THIS PARTY STARTED!!
//
//-----------------------------------------------------------------------------

// Validatiemails
var smtpConfig = {
	host: 'mail.hosting.indicia.nl',
	port: 25,
	secure:false,
	tls: {rejectUnauthorized: false},
	debug:true
};

var sendMailsToCkanUser = false;

var validatieMailConfig = {
	from: 'xxxxx', // sender address
	to: 'xxxxx', // list of receivers
};

// Source
var dpfOrganization = 'dataplatform';

// Offset/index to start at when importing
var totalNumberOfPackages;

// Context we are running in
var contextName;

// Stores the target packages, object so we can access by package id rather than look for it
var targetPackages = {};    

// CKAN client for dataplatform (SOURCE)
var ckan_dpf;

// CKAN client for data.overheid.nl (TARGET)
var ckan_donl;


//For the mappings
var organization_dataplatform;
var dataEigenaarMapping;
var dataThemaMapping;


//To store the dataplatform package ids that have been received (for stage 2)
var sourcePackageIds = [];

var logger = new logger();


// Check arguments and go if okay
if( process.argv[2]  ) {
    contextName = process.argv[2];
    
    if( !context[contextName+ '_dpf'] ) {
        console.log('Invalid context provided : node dataplatform_import.js [context|string]');
    }
    else {
        console.log("Running in context : " + contextName + '_dpf');
        console.log("CKAN Instance : " + context[contextName+ '_dpf'].api);

		sendMailsToCkanUser = context[contextName + '_dpf'].send_mails_to_ckan_user;

		if (context[contextName + '_dpf'].smtp_host) {
			smtpConfig.host = context[contextName + '_dpf'].smtp_host;
			console.log("Overriding default SMTP host. Now using: " + smtpConfig.host)
		}
		
        ckan_dpf  = new CKAN.Client('https://ckan.dataplatform.nl');
        ckan_donl       = new CKAN.Client(context[contextName + '_dpf'].api, context[contextName + '_dpf'].apikey);  
        ckan_donl_admin = new CKAN.Client(context[contextName + '_admin'].api, context[contextName + '_admin'].apikey);  

        validatieMailConfig.subject = 'Validatiefouten bij import Dataplatform naar ' + context[contextName + '_dpf'].api; // Subject line

        //Gogogo!
        loadMappings();
        
    }
}
else {
    console.log('Invalid parameters provided : node dataplatform_import.js [context|string]');
}

