//MANUALLLY ADDED

function myfunction(x){

	w = dw.wrangle()

	initial_transforms = dw.raw_inference(x).transforms;

	var data =dv.table(x);
		
	if(initial_transforms){
		initial_transforms.forEach(function(t){
			w.add(t);
		})
		w.apply([data]);
	}

//Manually added





w.add(dw.split().column(["data"])
	.table(0)
	.status("active")
	.drop(true)
	.result("row")
	.update(false)
	.insert_position("right")
	.row(undefined)
	.on("\n")
	.before(undefined)
	.after(undefined)
	.ignore_between(undefined)
	.which(1)
	.max(0)
	.positions(undefined)
	.quote_character(undefined)
)


w.add(dw.split().column(["data"])
	.table(0)
	.status("active")
	.drop(true)
	.result("column")
	.update(false)
	.insert_position("right")
	.row(undefined)
	.on(",")
	.before(undefined)
	.after(undefined)
	.ignore_between(undefined)
	.which(1)
	.max(0)
	.positions(undefined)
	.quote_character(undefined)
)

w.add(dw.drop().column(["split"])
	.table(0)
	.status("active")
	.drop(true)
)




//need to be changed as [] 
w.apply([data])



//MANUALLY added
return dw.wrangler_export(data,{});
}


//MANUALLY added
