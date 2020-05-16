var InvertedIndexRaw = "";
var DocumentRaw = "";
var Total = new Object();
var Frequency = new Object();
var Results = [];

ReadTotal();
ReadFrequency();
console.log(Total);
console.log(Frequency);

function ReadInvertedIndex()
{
	var file = new XMLHttpRequest();
	file.open("GET", "./data/InvertedIndex/part-r-00000", false);
	file.onreadystatechange = function ()
	{
		if (file.readyState === 4)
		{
			if (file.status === 200 || file.status == 0)
			{
				var text = file.responseText;
				InvertedIndexRaw = text;
			}
		}
	}

	file.send(null);
}

function ReadDocument(document_name)
{
	var file = new XMLHttpRequest();
	file.open("GET", "./data/Documents/" + document_name, false);
	file.onreadystatechange = function ()
	{
		if (file.readyState === 4)
		{
			if (file.status === 200 || file.status == 0)
			{
				var text = file.responseText;
				DocumentRaw = text;
			}
		}
	}

	file.send(null);
}

function ReadTotal()
{
	var file = new XMLHttpRequest();
	file.open("GET", "./data/Total/part-r-00000", false);
	file.onreadystatechange = function ()
	{
		if (file.readyState === 4)
		{
			if (file.status === 200 || file.status == 0)
			{
				var text = file.responseText;				
				var lines = text.split("\n");

				for (var i = 0; i < lines.length; i++)
				{
					var line = lines[i].split("\t");
					Total[line[0]] = line[1];
				}
			}
		}
	}

	file.send(null);
}

function ReadFrequency()
{
	var file = new XMLHttpRequest();
	file.open("GET", "./data/Frequency/part-r-00000", false);
	file.onreadystatechange = function ()
	{
		if (file.readyState === 4)
		{
			if (file.status === 200 || file.status == 0)
			{
				var text = file.responseText;				
				var lines = text.split("\n");

				for (var i = 0; i < lines.length; i++)
				{
					var line = lines[i].split("\t");
					Frequency[line[0]] = new Object();

					var frec_total = 0;
					var num_elements = line.length - 2;
					for (var j = 0; j < num_elements; j++)
					{
						var doc = line[j+1].substring(0, 37);
						var frec = line[j+1].substring(38, line[j+1].length);
						frec_total += parseInt(frec);
						Frequency[line[0]][doc] = frec;
					}

					Frequency[line[0]]["total"] = frec_total;
				}
			}
		}
	}

	file.send(null);
}

function TFIDF(word, uri)
{
	var tf_a = parseInt(Frequency[word][uri]);
	var tf_b = parseInt(Total[uri]);
	var idf_a = 10;
	var idf_b = parseInt(Frequency[word]["total"]);

	return (tf_a / tf_b) * Math.log(idf_a / idf_b);
}

function BubbleSort()
{
	var len = Results.length;

	for (var i = 0; i < len; i++)
	{
		for (var j = 0; j < len-i-1; j++)
		{
			if (parseInt(Results[j]["puntaje"]) > parseInt(Results[j+1]["puntaje"]))
			{
				var temp = Results[j];
				Results[j] = Results[j+1];
				Results[j+1] = temp;
			}
		}
	}
}

function Search(word)
{
	ReadInvertedIndex();
	var lines = InvertedIndexRaw.split("\n");

	for (var i = 0; i < lines.length; i++)
	{
		var line = lines[i].split("\t");

		if (line.length >= 2 && line[0] == word)
		{
			return line;
		}
	}

	return [];
}

function Show()
{
	var word = document.getElementById("textbox").value;
    var div = document.getElementById("results");
	var documents = Search(word);
	
	if (documents != [])
	{
		var code_table = "<table class='table'>";
		code_table += "<thead class='thead-dark'>";
		code_table += "<tr> <th scope='col'>Puntaje</th> <th scope='col'>URI</th> <th scope='col'>Nombre</th> <th scope='col'>Contenido</th> </tr>";
		code_table += "</thead>";
		code_table += "<tbody>";

		Results = [];
		var num_elements = documents.length - 2; // -1: documents[0]="palabra" | -1: documents[ultimo]=""
		for (var i = 0; i < num_elements; i++)
		{
			var uri = documents[i+1]; // hdfs://node1:9000/Documents/doc_2.txt
			var nombre = uri.substring(28, uri.length);

			ReadDocument(nombre);
			var contenido = DocumentRaw;

		    var puntaje = TFIDF(word, uri).toString();

			Results.push(new Object());
			Results[i]["uri"] = uri;
			Results[i]["nombre"] = nombre;
			Results[i]["contenido"] = contenido;
			Results[i]["puntaje"] = puntaje;
		}

		BubbleSort();

		for (var i = 0; i < Results.length; i++)
		{
			code_table += "<tr>";
		    code_table += "<td>" + Results[i]["puntaje"] + "</td>";
		    code_table += "<td>" + Results[i]["uri"] + "</td>";
		    code_table += "<td>" + Results[i]["nombre"] + "</td>";
		    code_table += "<td>" + Results[i]["contenido"] + "</td>";
		    code_table += "</tr>";
		}

		code_table += "</tbody>";
		code_table += "</table>"
		
		div.innerHTML = "";
		div.insertAdjacentHTML("beforeend", code_table);
	}
}
