package com.eventshop.eventshoplinux.domain.common;

import java.util.Arrays;

public class EmageElement {
	
	public String theme;
	public long startTime, endTime;
	public String startTimeStr, endTimeStr;
	public double min, max, latUnit, longUnit, swLat, swLong, neLat, neLong;
	public int row, col;
	public double[] image;
	public double value;
	public String mapEnabled;
	public String[] colors;
	
	public void setTheme(String th){
		this.theme = th;
	}
	public String getTheme(){
		return this.theme;
	}
	public void setStartTime(long st){
		this.startTime = st;
	}
	public long getStartTime(){
		return this.startTime;
	}
	public void setEndTime(long et){
		this.endTime = et;
	}
	public long getEndTime(){
		return this.endTime;
	}
	
	public void reduceSize(int level){
		double powLevel = Math.pow(2.0, level);
		double oriCol = this.col;
		this.row = (int) Math.ceil(this.row/powLevel);
		this.col = (int) Math.ceil(this.col/powLevel);
		this.latUnit = this.latUnit * powLevel;
		this.longUnit = this.longUnit * powLevel;
		double[] temp = new double[this.row*this.col];
		int count = 0;
		for(int i = 0; i < row; i++){
			for(int j = 0; j < col; j++){
				temp[count] = this.image[(int) ((powLevel*i*oriCol) + (powLevel*j))];

				//System.out.println(count + ", " + ((powLevel*i*oriCol) + (powLevel*j)));
				count++;
				
			}
		}
		this.image = temp;	
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("theme: " + theme);
		sb.append("\nrow: " + row);
		sb.append("\ncol: " + col);
		sb.append("\nlatUnit: " + latUnit);
		sb.append("\nlongUnit: " + longUnit);
		sb.append("\ncolors: " + Arrays.toString(colors));
		sb.append("\nimage: " +  Arrays.toString(image));
		return sb.toString();
	}
}
