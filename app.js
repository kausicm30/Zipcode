const csvtojson = require('csvtojson');
const csvWriter = require('csv-write-stream');
const fs=require('fs');
const csvReadFileName = 'InitialPostCode.csv';
const csvWriteFileName = 'FinalPostCode.csv';

var totalRows = 0;
var postCodeArray=[];
var startRow=0;
var endRow=0;

// if the exists means append the records otherwise create a csv file

if (!fs.existsSync(csvWriteFileName)) {
    var writer = csvWriter({sendHeaders: false});
    writer.pipe(fs.createWriteStream(csvWriteFileName));
    writer.write({
      header1: 'LOCALITY',
      header2: 'POSTCODE',
      header3: 'DISTRICT',
      header4: 'STATE'
    });
    writer.end();
}

//calculate total number of rows given in the read csv file
async function findTotalRowsInReadFile(){
    await csvtojson().fromFile(csvReadFileName).then(async (rows)=>{
        totalRows = await rows.length;
        console.log("Total Rows in the CSV file : "+totalRows);
        while(endRow!=totalRows){
            startRow = endRow;
            await getRecordsFromCSV();
        }
    });
}

findTotalRowsInReadFile();

//get all records of the each pincode
async function getRecordsFromCSV(){
    await csvtojson().fromFile(csvReadFileName).then(async (rows)=> {
            while((endRow != totalRows) && (rows[startRow]["Pincode"]== rows[endRow]["Pincode"])){
                var oneRow = {
                    postCode: rows[endRow]["Pincode"],
                    locality: rows[endRow]["Village/Locality name"],
                    district: rows[endRow]["Districtname"],
                    state: rows[endRow]["StateName"]
                };
                postCodeArray.push(oneRow);
                endRow++;
            }
            await removeDuplicates(postCodeArray).then(function(){
                console.log(startRow+" "+endRow);
                postCodeArray = [];
            });
            startRow= endRow;
    });
}

// remove duplicates from pincode array

async function removeDuplicates(postCodeArray){
    var uniquePostCodes =[];
    checkDuplicates(postCodeArray).then(async (uniquePostCodes)=>{
        var postcodes = uniquePostCodes;
        await storePostCode(postcodes)
    });
}

// check duplicates and remove
async function checkDuplicates(postCodeArray) {
    var uniquePostCodes = [];
    for(let postcode of postCodeArray) {
        let duplicate=false;
        for(let unique of uniquePostCodes) {
            if(unique.postCode == postcode.postCode){
                if(unique.state == postcode.state){
                    if(unique.district == postcode.district){
                        if(((unique.locality).toUpperCase()).includes((postcode.locality).toUpperCase()) ||((postcode.locality).toUpperCase()).includes((unique.locality).toUpperCase())){
                            duplicate = true;
                        }
                    }
                }
            }
        }
        if(!duplicate){
            uniquePostCodes.push(postcode);
        }
    }
    return uniquePostCodes;
}

// store all records after removing duplicates

function storePostCode (uniquePostCodes){
    for(let unique of uniquePostCodes){
        writer = csvWriter({sendHeaders: false});
        writer.pipe(fs.createWriteStream(csvWriteFileName, {flags: 'a'}));
        writer.write({
            header1: unique.locality,
            header2: unique.postCode,
            header3: unique.district,
            header4: unique.state
        });
        writer.end();
    }
}
