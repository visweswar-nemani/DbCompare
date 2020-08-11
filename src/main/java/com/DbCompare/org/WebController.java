package com.DbCompare.org;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.stereotype.Controller;


@Controller
public class WebController {
	
	
	
	
	
	@RequestMapping(value="/home",method=RequestMethod.GET )
	public String openPage() {
		return "home";
	}
	
	
	@RequestMapping(value="/mongoPage",method=RequestMethod.GET)
	public String mongoComputations() {
		return "mongoPage";
	}
	
	
	@RequestMapping(value="/hbasePage",method=RequestMethod.GET)
	public String hbaseComputations() {
		return "hbasePage";
	}
	
	
	
	

}
